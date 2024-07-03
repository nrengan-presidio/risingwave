// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::Context;
use create_sink::derive_default_column_project_for_sink;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_sqlparser::ast::{
    AlterTableOperation, ColumnOption, ConnectorSchema, Encode, ObjectName, Statement,
};
use risingwave_sqlparser::parser::Parser;

use super::create_source::get_json_schema_location;
use super::create_table::{bind_sql_columns, generate_stream_graph_for_table, ColumnIdGenerator};
use super::util::SourceSchemaCompatExt;
use super::{create_sink, HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::ExprImpl;
use crate::handler::create_sink::{fetch_incoming_sinks, insert_merger_to_union_with_project};
use crate::optimizer::plan_node::generic::SourceNodeKind;
use crate::optimizer::plan_node::{
    generic, LogicalSource, StreamProject, ToStream, ToStreamContext,
};
use crate::session::SessionImpl;
use crate::{Binder, OptimizerContext, TableCatalog, WithOptions};

pub async fn replace_table_with_definition(
    session: &Arc<SessionImpl>,
    table_name: ObjectName,
    definition: Statement,
    original_catalog: &Arc<TableCatalog>,
    source_schema: Option<ConnectorSchema>,
) -> Result<()> {
    // Create handler args as if we're creating a new table with the altered definition.
    let handler_args = HandlerArgs::new(session.clone(), &definition, Arc::from(""))?;
    let col_id_gen = ColumnIdGenerator::new_alter(original_catalog);
    let Statement::CreateTable {
        columns,
        constraints,
        source_watermarks,
        append_only,
        on_conflict,
        with_version_column,
        wildcard_idx,
        ..
    } = definition
    else {
        panic!("unexpected statement type: {:?}", definition);
    };

    let (mut graph, mut table, source) = generate_stream_graph_for_table(
        session,
        table_name,
        original_catalog,
        source_schema,
        handler_args.clone(),
        col_id_gen,
        columns.clone(),
        wildcard_idx,
        constraints,
        source_watermarks,
        append_only,
        on_conflict,
        with_version_column,
    )
    .await?;

    // Calculate the mapping from the original columns to the new columns.
    let col_index_mapping = ColIndexMapping::new(
        original_catalog
            .columns()
            .iter()
            .map(|old_c| {
                table.columns.iter().position(|new_c| {
                    new_c.get_column_desc().unwrap().column_id == old_c.column_id().get_id()
                })
            })
            .collect(),
        table.columns.len(),
    );

    let incoming_sink_ids: HashSet<_> = original_catalog.incoming_sinks.iter().copied().collect();
    let incoming_sinks = fetch_incoming_sinks(session, &incoming_sink_ids)?;
    let target_columns = bind_sql_columns(&columns)?;
    let default_columns: Vec<ExprImpl> = TableCatalog::default_column_exprs(&target_columns);

    for sink in incoming_sinks {
        let context = Rc::new(OptimizerContext::from_handler_args(handler_args.clone()));
        hijack_merger_for_target_table(
            &mut graph,
            &target_columns,
            &default_columns,
            &sink,
            context,
        )?;
    }

    table.incoming_sinks = incoming_sink_ids.iter().copied().collect();

    let catalog_writer = session.catalog_writer()?;

    catalog_writer
        .replace_table(source, table, graph, col_index_mapping)
        .await?;
    Ok(())
}

pub(crate) fn hijack_merger_for_target_table(
    graph: &mut StreamFragmentGraph,
    target_columns: &[ColumnCatalog],
    default_columns: &[ExprImpl],
    sink: &SinkCatalog,
    context: Rc<OptimizerContext>,
) -> Result<()> {
    let exprs = derive_default_column_project_for_sink(
        sink,
        &sink.full_schema(),
        target_columns,
        default_columns,
        false, // todo
    )?;

    let pb_project = StreamProject::new(generic::Project::new(
        exprs,
        LogicalSource::new(
            None,
            sink.full_columns().to_vec(),
            None,
            SourceNodeKind::CreateTable,
            context,
            None,
        )
        .and_then(|s| s.to_stream(&mut ToStreamContext::new(false)))?,
    ))
    .to_stream_prost_body_inner();

    for fragment in graph.fragments.values_mut() {
        if let Some(node) = &mut fragment.node {
            insert_merger_to_union_with_project(
                node,
                &pb_project,
                &format!(
                    "{}.{}.{}",
                    sink.database_id.database_id, sink.schema_id.schema_id, sink.name
                ),
            );
        }
    }

    Ok(())
}

/// Handle `ALTER TABLE [ADD|DROP] COLUMN` statements. The `operation` must be either `AddColumn` or
/// `DropColumn`.
pub async fn handle_alter_table_column(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    operation: AlterTableOperation,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let original_catalog = fetch_table_catalog_for_alter(session.as_ref(), &table_name)?;
    // if !original_catalog.incoming_sinks.is_empty() {
    //     bail_not_implemented!("alter table with incoming sinks");
    // }

    // TODO(yuhao): alter table with generated columns.
    if original_catalog.has_generated_column() {
        return Err(RwError::from(ErrorCode::BindError(
            "Alter a table with generated column has not been implemented.".to_string(),
        )));
    }

    // Retrieve the original table definition and parse it to AST.
    let [mut definition]: [_; 1] = Parser::parse_sql(&original_catalog.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();
    let Statement::CreateTable {
        columns,
        source_schema,
        ..
    } = &mut definition
    else {
        panic!("unexpected statement: {:?}", definition);
    };
    let source_schema = source_schema
        .clone()
        .map(|source_schema| source_schema.into_v2_with_warning());

    if let Some(source_schema) = &source_schema {
        if schema_has_schema_registry(source_schema) {
            return Err(ErrorCode::NotSupported(
                "alter table with schema registry".to_string(),
                "try `ALTER TABLE .. FORMAT .. ENCODE .. (...)` instead".to_string(),
            )
            .into());
        }
    }

    match operation {
        AlterTableOperation::AddColumn {
            column_def: new_column,
        } => {
            // Duplicated names can actually be checked by `StreamMaterialize`. We do here for
            // better error reporting.
            let new_column_name = new_column.name.real_value();
            if columns
                .iter()
                .any(|c| c.name.real_value() == new_column_name)
            {
                Err(ErrorCode::InvalidInputSyntax(format!(
                    "column \"{new_column_name}\" of table \"{table_name}\" already exists"
                )))?
            }

            if new_column
                .options
                .iter()
                .any(|x| matches!(x.option, ColumnOption::GeneratedColumns(_)))
            {
                Err(ErrorCode::InvalidInputSyntax(
                    "alter table add generated columns is not supported".to_string(),
                ))?
            }

            // Add the new column to the table definition.
            columns.push(new_column);
        }

        AlterTableOperation::DropColumn {
            column_name,
            if_exists,
            cascade,
        } => {
            if cascade {
                bail_not_implemented!(issue = 6903, "drop column cascade");
            }

            // Locate the column by name and remove it.
            let column_name = column_name.real_value();
            let removed_column = columns
                .extract_if(|c| c.name.real_value() == column_name)
                .at_most_one()
                .ok()
                .unwrap();

            if removed_column.is_some() {
                // PASS
            } else if if_exists {
                return Ok(PgResponse::builder(StatementType::ALTER_TABLE)
                    .notice(format!(
                        "column \"{}\" does not exist, skipping",
                        column_name
                    ))
                    .into());
            } else {
                Err(ErrorCode::InvalidInputSyntax(format!(
                    "column \"{}\" of table \"{}\" does not exist",
                    column_name, table_name
                )))?
            }
        }

        _ => unreachable!(),
    }

    replace_table_with_definition(
        &session,
        table_name,
        definition,
        &original_catalog,
        source_schema,
    )
    .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}

pub fn schema_has_schema_registry(schema: &ConnectorSchema) -> bool {
    match schema.row_encode {
        Encode::Avro | Encode::Protobuf => true,
        Encode::Json => {
            let mut options = WithOptions::try_from(schema.row_options()).unwrap();
            matches!(get_json_schema_location(options.inner_mut()), Ok(Some(_)))
        }
        _ => false,
    }
}

pub fn fetch_table_catalog_for_alter(
    session: &SessionImpl,
    table_name: &ObjectName,
) -> Result<Arc<TableCatalog>> {
    let db_name = session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let original_catalog = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) =
            reader.get_table_by_name(db_name, schema_path, &real_table_name)?;

        match table.table_type() {
            TableType::Table => {}

            _ => Err(ErrorCode::InvalidInputSyntax(format!(
                "\"{table_name}\" is not a table or cannot be altered"
            )))?,
        }

        session.check_privilege_for_drop_alter(schema_name, &**table)?;

        table.clone()
    };

    Ok(original_catalog)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, ROWID_PREFIX};
    use risingwave_common::types::DataType;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_add_column_handler() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let sql = "create table t (i int, r real);";
        frontend.run_sql(sql).await.unwrap();

        let get_table = || {
            let catalog_reader = session.env().catalog_reader().read_guard();
            catalog_reader
                .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
                .unwrap()
                .0
                .clone()
        };

        let table = get_table();

        let columns: HashMap<_, _> = table
            .columns
            .iter()
            .map(|col| (col.name(), (col.data_type().clone(), col.column_id())))
            .collect();

        // Alter the table.
        let sql = "alter table t add column s text;";
        frontend.run_sql(sql).await.unwrap();

        let altered_table = get_table();

        let altered_columns: HashMap<_, _> = altered_table
            .columns
            .iter()
            .map(|col| (col.name(), (col.data_type().clone(), col.column_id())))
            .collect();

        // Check the new column.
        assert_eq!(columns.len() + 1, altered_columns.len());
        assert_eq!(altered_columns["s"].0, DataType::Varchar);

        // Check the old columns and IDs are not changed.
        assert_eq!(columns["i"], altered_columns["i"]);
        assert_eq!(columns["r"], altered_columns["r"]);
        assert_eq!(columns[ROWID_PREFIX], altered_columns[ROWID_PREFIX]);

        // Check the version is updated.
        assert_eq!(
            table.version.as_ref().unwrap().version_id + 1,
            altered_table.version.as_ref().unwrap().version_id
        );
        assert_eq!(
            table.version.as_ref().unwrap().next_column_id.next(),
            altered_table.version.as_ref().unwrap().next_column_id
        );
    }
}
