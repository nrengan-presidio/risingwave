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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::fifo_cache::{CacheItem, CacheKey, CacheValue};

pub struct SmallHotCache<K: CacheKey, V: CacheValue> {
    queue: VecDeque<Box<CacheItem<K, V>>>,
    cost: Arc<AtomicUsize>,
    capacity: usize,
}

impl<K: CacheKey, V: CacheValue> SmallHotCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            cost: Arc::new(AtomicUsize::new(0)),
            capacity,
        }
    }

    pub fn get_size_counter(&self) -> Arc<AtomicUsize> {
        self.cost.clone()
    }

    #[inline(always)]
    pub fn size(&self) -> usize {
        self.cost.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn is_full(&self) -> bool {
        self.size() > self.capacity
    }

    pub fn count(&self) -> usize {
        self.queue.len()
    }

    pub fn evict(&mut self) -> Option<Box<CacheItem<K, V>>> {
        let mut item = self.queue.pop_front()?;
        self.cost
            .fetch_sub(item.cost(), std::sync::atomic::Ordering::Release);
        item.unmark();
        Some(item)
    }

    pub fn insert(&mut self, mut item: Box<CacheItem<K, V>>) {
        item.mark_small();
        self.cost
            .fetch_add(item.cost(), std::sync::atomic::Ordering::Release);
        self.queue.push_back(item);
    }

    pub fn clear(&mut self) {
        self.queue.clear();
        self.cost.store(0, Ordering::Release);
    }
}