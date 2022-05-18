// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::AddAssign;
use std::sync::Arc;

use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::{Array, ArrayBuilder, DataChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::error::RwError;
use risingwave_common::types::Scalar;
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;

use crate::executor2::{BoxedDataChunkStream, Executor2};

pub struct GenerateSeriesI32Executor2<T: Array, S: Array> {
    start: T::OwnedItem,
    stop: T::OwnedItem,
    step: S::OwnedItem,

    schema: Schema,
    identity: String,
}

impl<T: Array, S: Array> GenerateSeriesI32Executor2<T, S> {
    pub fn new(
        start: T::OwnedItem,
        stop: T::OwnedItem,
        step: S::OwnedItem,
        schema: Schema,
        identity: String,
    ) -> Self {
        Self {
            start,
            stop,
            step,
            schema,
            identity,
        }
    }
}

impl<T: Array, S: Array> Executor2 for GenerateSeriesI32Executor2<T, S>
where
    T::OwnedItem: PartialOrd<T::OwnedItem>,
    T::OwnedItem: for<'a> AddAssign<S::RefItem<'a>>,
{
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl<T, S> GenerateSeriesI32Executor2<T, S>
where
    T: Array,
    S: Array,
    T::OwnedItem: PartialOrd<T::OwnedItem>,
    T::OwnedItem: for<'a> AddAssign<S::RefItem<'a>>,
{
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            start, stop, step, ..
        } = *self;

        // let mut rest_rows = ((stop - start) / step + 1) as usize;
        let mut cur = start;

        // Simulate a do-while loop.
        while cur <= stop {
            let chunk_size = DEFAULT_CHUNK_BUFFER_SIZE;
            let mut builder = T::Builder::new(chunk_size)?;

            for _ in 0..chunk_size {
                if cur > stop {
                    break;
                }
                builder.append(Some(cur.as_scalar_ref())).unwrap();
                cur += step.as_scalar_ref();
            }

            let arr = builder.finish()?;
            let columns = vec![Column::new(Arc::new(arr.into()))];
            let chunk: DataChunk = DataChunk::builder().columns(columns).build();

            yield chunk;
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::{Array, ArrayImpl, I32Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::try_match_expand;
    use risingwave_common::types::DataType;

    use super::*;

    #[tokio::test]
    async fn test_generate_series() {
        generate_series_test_case(2, 4, 1).await;
        generate_series_test_case(0, 9, 2).await;
        generate_series_test_case(0, (DEFAULT_CHUNK_BUFFER_SIZE * 2 + 3) as i32, 1).await;
    }

    async fn generate_series_test_case(start: i32, stop: i32, step: i32) {
        let executor = Box::new(GenerateSeriesI32Executor2::<I32Array, I32Array> {
            start,
            stop,
            step,
            schema: Schema::new(vec![Field::unnamed(DataType::Int32)]),
            identity: "GenerateSeriesI32Executor2".to_string(),
        });
        let mut remained_values = ((stop - start) / step + 1) as usize;
        let mut stream = executor.execute();
        while remained_values > 0 {
            let chunk = stream.next().await.unwrap().unwrap();
            let col = chunk.column_at(0);
            let arr = try_match_expand!(col.array_ref(), ArrayImpl::Int32).unwrap();

            if remained_values > DEFAULT_CHUNK_BUFFER_SIZE {
                assert_eq!(arr.len(), DEFAULT_CHUNK_BUFFER_SIZE);
            } else {
                assert_eq!(arr.len(), remained_values);
            }
            remained_values -= arr.len();
        }
        assert!(stream.next().await.is_none());
    }
}
