use std::collections::{HashMap, VecDeque};

use anyhow::anyhow;
use risingwave_hummock_sdk::time_travel::{
    refill_version, IncompleteHummockVersion, IncompleteHummockVersionDelta,
};
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{HummockEpoch, HummockSstableId, HummockSstableObjectId};
use risingwave_meta_model_v2::{
    hummock_epoch_to_version, hummock_sstable_info, hummock_time_travel_delta,
    hummock_time_travel_version,
};
use risingwave_pb::hummock::{PbHummockVersion, PbHummockVersionDelta, PbSstableInfo};
use sea_orm::sea_query::OnConflict;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
};

use crate::controller::SqlMetaStore;
use crate::hummock::error::{Error, Result};
use crate::hummock::HummockManager;
use crate::manager::MetaStoreImpl;

/// Time travel.
impl HummockManager {
    pub(crate) fn sql_store(&self) -> Result<SqlMetaStore> {
        match self.env.meta_store() {
            MetaStoreImpl::Sql(sql_store) => Ok(sql_store),
            _ => Err(anyhow!("time travel requires SQL meta store").into()),
        }
    }

    pub(crate) async fn truncate_time_travel_metadata(
        &self,
        epoch_watermark: HummockEpoch,
    ) -> Result<()> {
        let sql_store = self.sql_store()?;
        let txn = sql_store.conn.begin().await?;

        let version_watermark = hummock_epoch_to_version::Entity::find()
            .filter(
                hummock_epoch_to_version::Column::Epoch
                    .lt(risingwave_meta_model_v2::Epoch::try_from(epoch_watermark).unwrap()),
            )
            .order_by_desc(hummock_epoch_to_version::Column::Epoch)
            .one(&txn)
            .await?;
        let Some(version_watermark) = version_watermark else {
            return Ok(());
        };
        let res = hummock_epoch_to_version::Entity::delete_many()
            .filter(
                hummock_epoch_to_version::Column::Epoch
                    .lt(risingwave_meta_model_v2::Epoch::try_from(epoch_watermark).unwrap()),
            )
            .exec(&txn)
            .await?;
        tracing::debug!(
            epoch_watermark,
            "delete {} rows from hummock_epoch_to_version",
            res.rows_affected
        );

        let earliest_valid_version_id: Option<risingwave_meta_model_v2::HummockVersionId> =
            hummock_time_travel_version::Entity::find()
                .select_only()
                .column(hummock_time_travel_version::Column::VersionId)
                .filter(
                    hummock_time_travel_version::Column::VersionId
                        .lte(version_watermark.version_id),
                )
                .order_by_desc(hummock_time_travel_version::Column::VersionId)
                .into_tuple()
                .one(&txn)
                .await?;
        let Some(earliest_valid_version_id) = earliest_valid_version_id else {
            txn.commit().await?;
            return Ok(());
        };
        let version_ids_to_delete: Vec<risingwave_meta_model_v2::HummockVersionId> =
            hummock_time_travel_version::Entity::find()
                .select_only()
                .column(hummock_time_travel_version::Column::VersionId)
                .filter(
                    hummock_time_travel_version::Column::VersionId.lt(earliest_valid_version_id),
                )
                .order_by_desc(hummock_time_travel_version::Column::VersionId)
                .into_tuple()
                .all(&txn)
                .await?;
        let delta_ids_to_delete: Vec<risingwave_meta_model_v2::HummockVersionId> =
            hummock_time_travel_delta::Entity::find()
                .select_only()
                .column(hummock_time_travel_delta::Column::VersionId)
                .filter(hummock_time_travel_delta::Column::VersionId.lt(earliest_valid_version_id))
                .into_tuple()
                .all(&txn)
                .await?;

        let earliest_valid_version_sst_ids = {
            let earliest_valid_version =
                hummock_time_travel_version::Entity::find_by_id(earliest_valid_version_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| {
                        Error::TimeTravel(anyhow!(format!(
                            "version {} not found",
                            earliest_valid_version_id
                        )))
                    })?;
            HummockVersion::from_persisted_protobuf(&earliest_valid_version.version.to_protobuf())
                .get_sst_ids()
        };

        for delta_id_to_delete in delta_ids_to_delete {
            let delta_to_delete = hummock_time_travel_delta::Entity::find_by_id(delta_id_to_delete)
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    Error::TimeTravel(anyhow!(format!(
                        "version delta {} not found",
                        delta_id_to_delete
                    )))
                })?;
            let new_sst_ids = HummockVersionDelta::from_persisted_protobuf(
                &delta_to_delete.version_delta.to_protobuf(),
            )
            .newly_added_sst_ids();
            // The SST ids added and then deleted by compaction between the 2 versions.
            let sst_ids_to_delete = &new_sst_ids - &earliest_valid_version_sst_ids;
            let res = hummock_sstable_info::Entity::delete_many()
                .filter(hummock_sstable_info::Column::SstId.is_in(sst_ids_to_delete))
                .exec(&txn)
                .await?;
            tracing::debug!(
                delta_id = delta_to_delete.version_id,
                "delete {} rows from hummock_sstable_info",
                res.rows_affected
            );
        }

        let mut next_version_sst_ids = earliest_valid_version_sst_ids;
        for prev_version_id in version_ids_to_delete {
            let sst_ids = {
                let prev_version = hummock_time_travel_version::Entity::find_by_id(prev_version_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| {
                        Error::TimeTravel(anyhow!(format!(
                            "prev_version {} not found",
                            prev_version_id
                        )))
                    })?;
                HummockVersion::from_persisted_protobuf(&prev_version.version.to_protobuf())
                    .get_sst_ids()
            };
            // The SST ids deleted by compaction between the 2 versions.
            let sst_ids_to_delete = &sst_ids - &next_version_sst_ids;
            let res = hummock_sstable_info::Entity::delete_many()
                .filter(hummock_sstable_info::Column::SstId.is_in(sst_ids_to_delete))
                .exec(&txn)
                .await?;
            tracing::debug!(
                prev_version_id,
                "delete {} rows from hummock_sstable_info",
                res.rows_affected
            );
            next_version_sst_ids = sst_ids;
        }

        let res = hummock_time_travel_version::Entity::delete_many()
            .filter(hummock_time_travel_version::Column::VersionId.lt(earliest_valid_version_id))
            .exec(&txn)
            .await?;
        tracing::debug!(
            epoch_watermark_version_id = version_watermark.version_id,
            earliest_valid_version_id,
            "delete {} rows from hummock_time_travel_version",
            res.rows_affected
        );

        let res = hummock_time_travel_delta::Entity::delete_many()
            .filter(hummock_time_travel_delta::Column::VersionId.lt(earliest_valid_version_id))
            .exec(&txn)
            .await?;
        tracing::debug!(
            epoch_watermark_version_id = version_watermark.version_id,
            earliest_valid_version_id,
            "delete {} rows from hummock_time_travel_delta",
            res.rows_affected
        );

        txn.commit().await?;
        Ok(())
    }

    pub(crate) async fn all_object_ids_in_time_travel(
        &self,
    ) -> crate::hummock::error::Result<impl Iterator<Item = HummockSstableId>> {
        let sql_store = self.sql_store()?;
        let object_ids: Vec<risingwave_meta_model_v2::HummockSstableObjectId> =
            hummock_sstable_info::Entity::find()
                .select_only()
                .column(hummock_sstable_info::Column::ObjectId)
                .into_tuple()
                .all(&sql_store.conn)
                .await?;
        let object_ids = object_ids
            .into_iter()
            .map(|object_id| HummockSstableObjectId::try_from(object_id).unwrap());
        Ok(object_ids)
    }

    /// Attempt to locate the version corresponding to `query_epoch`.
    ///
    /// The version is retrieved from `hummock_epoch_to_version`, selecting the entry with the largest epoch that's lte `query_epoch`.
    ///
    /// The resulted version is complete, i.e. with correct `SstableInfo`.
    pub async fn epoch_to_version(
        &self,
        query_epoch: HummockEpoch,
    ) -> crate::hummock::error::Result<HummockVersion> {
        let sql_store = self.sql_store()?;
        let epoch_to_version = hummock_epoch_to_version::Entity::find()
            .filter(
                hummock_epoch_to_version::Column::Epoch
                    .lte(risingwave_meta_model_v2::Epoch::try_from(query_epoch).unwrap()),
            )
            .order_by_desc(hummock_epoch_to_version::Column::Epoch)
            .one(&sql_store.conn)
            .await?
            .ok_or_else(|| {
                Error::TimeTravel(anyhow!(format!(
                    "version not found for epoch {}",
                    query_epoch
                )))
            })?;
        let expected_version_id = epoch_to_version.version_id;

        let replay_version = hummock_time_travel_version::Entity::find()
            .filter(hummock_time_travel_version::Column::VersionId.lte(expected_version_id))
            .order_by_desc(hummock_time_travel_version::Column::VersionId)
            .one(&sql_store.conn)
            .await?
            .ok_or_else(|| {
                Error::TimeTravel(anyhow!(format!(
                    "no replay version found for epoch {}, version {}",
                    query_epoch, expected_version_id,
                )))
            })?;
        let deltas = hummock_time_travel_delta::Entity::find()
            .filter(hummock_time_travel_delta::Column::VersionId.gt(replay_version.version_id))
            .filter(hummock_time_travel_delta::Column::VersionId.lte(expected_version_id))
            .order_by_asc(hummock_time_travel_delta::Column::VersionId)
            .all(&sql_store.conn)
            .await?;
        let mut actual_version = replay_archive(
            replay_version.version.to_protobuf(),
            deltas.into_iter().map(|d| d.version_delta.to_protobuf()),
        );

        let mut sst_ids = actual_version
            .get_sst_ids()
            .into_iter()
            .collect::<VecDeque<_>>();
        let sst_count = sst_ids.len();
        let mut sst_id_to_info = HashMap::with_capacity(sst_count);
        let sst_info_fetch_batch_size = std::env::var("RW_TIME_TRAVEL_SST_INFO_FETCH_BATCH_SIZE")
            .unwrap_or_else(|_| "100".into())
            .parse()
            .unwrap();
        while !sst_ids.is_empty() {
            let sst_infos = hummock_sstable_info::Entity::find()
                .filter(hummock_sstable_info::Column::SstId.is_in(
                    sst_ids.drain(..std::cmp::min(sst_info_fetch_batch_size, sst_ids.len())),
                ))
                .all(&sql_store.conn)
                .await?;
            for sst_info in sst_infos {
                let sst_info = sst_info.sstable_info.to_protobuf();
                sst_id_to_info.insert(sst_info.sst_id, sst_info);
            }
        }
        if sst_count != sst_id_to_info.len() {
            return Err(Error::TimeTravel(anyhow!(format!(
                "some SstableInfos not found for epoch {}, version {}",
                query_epoch, expected_version_id,
            ))));
        }
        refill_version(&mut actual_version, &sst_id_to_info);
        Ok(actual_version)
    }

    pub(crate) async fn write_time_travel_metadata(
        &self,
        txn: &DatabaseTransaction,
        version: Option<&HummockVersion>,
        delta: HummockVersionDelta,
    ) -> Result<()> {
        async fn write_sstable_infos(
            sst_infos: impl Iterator<Item = &PbSstableInfo>,
            txn: &DatabaseTransaction,
        ) -> Result<()> {
            for sst_info in sst_infos {
                let m = hummock_sstable_info::ActiveModel {
                    sst_id: Set(sst_info.sst_id.try_into().unwrap()),
                    object_id: Set(sst_info.object_id.try_into().unwrap()),
                    sstable_info: Set(sst_info.into()),
                };
                hummock_sstable_info::Entity::insert(m)
                    .on_conflict(
                        OnConflict::column(hummock_sstable_info::Column::SstId)
                            .do_nothing()
                            .to_owned(),
                    )
                    .do_nothing()
                    .exec(txn)
                    .await?;
            }
            Ok(())
        }

        let epoch = delta.max_committed_epoch;
        let version_id = delta.id;
        let m = hummock_epoch_to_version::ActiveModel {
            epoch: Set(epoch.try_into().unwrap()),
            version_id: Set(version_id.try_into().unwrap()),
        };
        hummock_epoch_to_version::Entity::insert(m)
            .exec(txn)
            .await?;

        if let Some(version) = version {
            write_sstable_infos(version.get_sst_infos(), &txn).await?;
            let m = hummock_time_travel_version::ActiveModel {
                version_id: Set(
                    risingwave_meta_model_v2::HummockVersionId::try_from(version.id).unwrap(),
                ),
                version: Set((&IncompleteHummockVersion::from(version).to_protobuf()).into()),
            };
            hummock_time_travel_version::Entity::insert(m)
                .on_conflict(
                    OnConflict::column(hummock_time_travel_version::Column::VersionId)
                        .do_nothing()
                        .to_owned(),
                )
                .do_nothing()
                .exec(txn)
                .await?;
        }
        write_sstable_infos(delta.newly_added_sst_infos(), txn).await?;
        let m = hummock_time_travel_delta::ActiveModel {
            version_id: Set(
                risingwave_meta_model_v2::HummockVersionId::try_from(delta.id).unwrap(),
            ),
            version_delta: Set((&IncompleteHummockVersionDelta::from(&delta).to_protobuf()).into()),
        };
        hummock_time_travel_delta::Entity::insert(m)
            .on_conflict(
                OnConflict::column(hummock_time_travel_delta::Column::VersionId)
                    .do_nothing()
                    .to_owned(),
            )
            .do_nothing()
            .exec(txn)
            .await?;

        Ok(())
    }
}

fn replay_archive(
    version: PbHummockVersion,
    deltas: impl Iterator<Item = PbHummockVersionDelta>,
) -> HummockVersion {
    let mut last_version = HummockVersion::from_persisted_protobuf(&version);
    for d in deltas {
        let d = HummockVersionDelta::from_persisted_protobuf(&d);
        // Need to work around the assertion in `apply_version_delta`.
        // Because compaction deltas are not included in time travel archive.
        while last_version.id < d.prev_id {
            last_version.id += 1;
        }
        last_version.apply_version_delta(&d);
    }
    last_version
}
