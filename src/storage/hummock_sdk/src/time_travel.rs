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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::group_delta::DeltaType;
use risingwave_pb::hummock::hummock_version::PbLevels;
use risingwave_pb::hummock::hummock_version_delta::{ChangeLogDelta, PbGroupDeltas};
use risingwave_pb::hummock::{
    EpochNewChangeLog, PbGroupDelta, PbHummockVersion, PbHummockVersionDelta, PbIntraLevelDelta,
    PbLevel, PbOverlappingLevel, PbSstableInfo, SstableInfo, StateTableInfoDelta,
};

use crate::change_log::TableChangeLog;
use crate::table_watermark::TableWatermarks;
use crate::version::{HummockVersion, HummockVersionDelta, HummockVersionStateTableInfo};
use crate::{CompactionGroupId, HummockSstableId};

/// [`IncompleteHummockVersion`] is incomplete because `SSTableInfo` only has the `sst_id` set in the following fields:
/// - `PbLevels`
/// - `TableChangeLog`
#[derive(Debug, Clone, PartialEq)]
pub struct IncompleteHummockVersion {
    pub id: u64,
    pub levels: HashMap<CompactionGroupId, PbLevels>,
    pub max_committed_epoch: u64,
    safe_epoch: u64,
    pub table_watermarks: HashMap<TableId, Arc<TableWatermarks>>,
    pub table_change_log: HashMap<TableId, TableChangeLog>,
    pub state_table_info: HummockVersionStateTableInfo,
}

/// Clone from an `SstableInfo`, but only set the `sst_id` for the target, leaving other fields as default.
fn stripped_sstable_info(origin: &SstableInfo) -> SstableInfo {
    SstableInfo {
        object_id: Default::default(),
        sst_id: origin.sst_id,
        key_range: Default::default(),
        file_size: Default::default(),
        table_ids: Default::default(),
        meta_offset: Default::default(),
        stale_key_count: Default::default(),
        total_key_count: Default::default(),
        min_epoch: Default::default(),
        max_epoch: Default::default(),
        uncompressed_file_size: Default::default(),
        range_tombstone_count: Default::default(),
        bloom_filter_kind: Default::default(),
    }
}

fn stripped_epoch_new_change_log(origin: &EpochNewChangeLog) -> EpochNewChangeLog {
    EpochNewChangeLog {
        old_value: origin.old_value.iter().map(stripped_sstable_info).collect(),
        new_value: origin.new_value.iter().map(stripped_sstable_info).collect(),
        epochs: origin.epochs.clone(),
    }
}

fn stripped_change_log_delta(origin: &ChangeLogDelta) -> ChangeLogDelta {
    ChangeLogDelta {
        new_log: origin.new_log.as_ref().map(stripped_epoch_new_change_log),
        truncate_epoch: origin.truncate_epoch,
    }
}

fn stripped_level(origin: &PbLevel) -> PbLevel {
    PbLevel {
        level_idx: origin.level_idx,
        level_type: origin.level_type,
        table_infos: origin
            .table_infos
            .iter()
            .map(stripped_sstable_info)
            .collect(),
        total_file_size: origin.total_file_size,
        sub_level_id: origin.sub_level_id,
        uncompressed_file_size: origin.uncompressed_file_size,
        vnode_partition_count: origin.vnode_partition_count,
    }
}

pub fn refill_version(
    version: &mut HummockVersion,
    sst_id_to_info: &HashMap<HummockSstableId, PbSstableInfo>,
) {
    for level in version.levels.values_mut().flat_map(|level| {
        level
            .l0
            .as_mut()
            .unwrap()
            .sub_levels
            .iter_mut()
            .rev()
            .chain(level.levels.iter_mut())
    }) {
        refill_level(level, sst_id_to_info);
    }

    for t in version.table_change_log.values_mut() {
        refill_table_change_log(t, sst_id_to_info);
    }
}

fn refill_level(level: &mut PbLevel, sst_id_to_info: &HashMap<HummockSstableId, PbSstableInfo>) {
    for s in &mut level.table_infos {
        refill_sstable_info(s, sst_id_to_info);
    }
}

fn refill_table_change_log(
    table_change_log: &mut TableChangeLog,
    sst_id_to_info: &HashMap<HummockSstableId, PbSstableInfo>,
) {
    for c in &mut table_change_log.0 {
        for s in &mut c.old_value {
            refill_sstable_info(s, sst_id_to_info);
        }
        for s in &mut c.new_value {
            refill_sstable_info(s, sst_id_to_info);
        }
    }
}

/// Caller should ensure `sst_id_to_info` includes an entry corresponding to `sstable_info`.
fn refill_sstable_info(
    sstable_info: &mut PbSstableInfo,
    sst_id_to_info: &HashMap<HummockSstableId, PbSstableInfo>,
) {
    *sstable_info = sst_id_to_info
        .get(&sstable_info.sst_id)
        .unwrap_or_else(|| panic!("SstableInfo should exist"))
        .clone();
}

fn stripped_l0(origin: &PbOverlappingLevel) -> PbOverlappingLevel {
    PbOverlappingLevel {
        sub_levels: origin.sub_levels.iter().map(stripped_level).collect(),
        total_file_size: origin.total_file_size,
        uncompressed_file_size: origin.uncompressed_file_size,
    }
}

#[allow(deprecated)]
fn stripped_levels(origin: &PbLevels) -> PbLevels {
    PbLevels {
        levels: origin.levels.iter().map(stripped_level).collect(),
        l0: origin.l0.as_ref().map(stripped_l0),
        group_id: origin.group_id,
        parent_group_id: origin.parent_group_id,
        member_table_ids: Default::default(),
    }
}

fn stripped_intra_level_delta(origin: &PbIntraLevelDelta) -> PbIntraLevelDelta {
    PbIntraLevelDelta {
        level_idx: origin.level_idx,
        l0_sub_level_id: origin.l0_sub_level_id,
        removed_table_ids: origin.removed_table_ids.clone(),
        inserted_table_infos: origin
            .inserted_table_infos
            .iter()
            .map(stripped_sstable_info)
            .collect(),
        vnode_partition_count: origin.vnode_partition_count,
    }
}

fn stripped_group_delta(origin: &PbGroupDelta) -> PbGroupDelta {
    let delta_type = origin.delta_type.as_ref().map(|d| match d {
        DeltaType::IntraLevel(l) => DeltaType::IntraLevel(stripped_intra_level_delta(l)),
        DeltaType::GroupConstruct(l) => DeltaType::GroupConstruct(l.clone()),
        DeltaType::GroupDestroy(l) => DeltaType::GroupDestroy(l.clone()),
        DeltaType::GroupMetaChange(_) => unreachable!(""),
        DeltaType::GroupTableChange(_) => unreachable!(""),
    });
    PbGroupDelta { delta_type }
}

fn stripped_group_deltas(origin: &PbGroupDeltas) -> PbGroupDeltas {
    let group_deltas = origin
        .group_deltas
        .iter()
        .map(stripped_group_delta)
        .collect();
    PbGroupDeltas { group_deltas }
}

/// `SStableInfo` will be stripped.
impl From<&HummockVersion> for IncompleteHummockVersion {
    fn from(version: &HummockVersion) -> Self {
        Self {
            id: version.id,
            levels: version
                .levels
                .iter()
                .map(|(group_id, levels)| (*group_id as CompactionGroupId, stripped_levels(levels)))
                .collect(),
            max_committed_epoch: version.max_committed_epoch,
            safe_epoch: version.visible_table_safe_epoch(),
            table_watermarks: version.table_watermarks.clone(),
            table_change_log: version
                .table_change_log
                .iter()
                .map(|(table_id, change_log)| {
                    let incomplete_table_change_log = change_log
                        .0
                        .iter()
                        .map(stripped_epoch_new_change_log)
                        .collect();
                    (*table_id, TableChangeLog(incomplete_table_change_log))
                })
                .collect(),
            state_table_info: version.state_table_info.clone(),
        }
    }
}

impl IncompleteHummockVersion {
    /// Resulted `SStableInfo` is incompelte.
    pub fn to_protobuf(&self) -> PbHummockVersion {
        PbHummockVersion {
            id: self.id,
            levels: self
                .levels
                .iter()
                .map(|(group_id, levels)| (*group_id as _, levels.clone()))
                .collect(),
            max_committed_epoch: self.max_committed_epoch,
            safe_epoch: self.safe_epoch,
            table_watermarks: self
                .table_watermarks
                .iter()
                .map(|(table_id, watermark)| (table_id.table_id, watermark.to_protobuf()))
                .collect(),
            table_change_logs: self
                .table_change_log
                .iter()
                .map(|(table_id, change_log)| (table_id.table_id, change_log.to_protobuf()))
                .collect(),
            state_table_info: self.state_table_info.to_protobuf(),
        }
    }
}

/// [`IncompleteHummockVersionDelta`] is incomplete because `SSTableInfo` only has the `sst_id` set in the following fields:
/// - `PbGroupDeltas`
/// - `ChangeLogDelta`
#[derive(Debug, PartialEq, Clone)]
pub struct IncompleteHummockVersionDelta {
    pub id: u64,
    pub prev_id: u64,
    pub group_deltas: HashMap<CompactionGroupId, PbGroupDeltas>,
    pub max_committed_epoch: u64,
    pub safe_epoch: u64,
    pub trivial_move: bool,
    pub new_table_watermarks: HashMap<TableId, TableWatermarks>,
    pub removed_table_ids: HashSet<TableId>,
    pub change_log_delta: HashMap<TableId, ChangeLogDelta>,
    pub state_table_info_delta: HashMap<TableId, StateTableInfoDelta>,
}

/// `SStableInfo` will be stripped.
impl From<&HummockVersionDelta> for IncompleteHummockVersionDelta {
    fn from(delta: &HummockVersionDelta) -> Self {
        Self {
            id: delta.id,
            prev_id: delta.prev_id,
            group_deltas: delta
                .group_deltas
                .iter()
                .map(|(cg_id, deltas)| (*cg_id, stripped_group_deltas(deltas)))
                .collect(),
            max_committed_epoch: delta.max_committed_epoch,
            safe_epoch: delta.safe_epoch,
            trivial_move: delta.trivial_move,
            new_table_watermarks: delta.new_table_watermarks.clone(),
            removed_table_ids: delta.removed_table_ids.clone(),
            change_log_delta: delta
                .change_log_delta
                .iter()
                .map(|(table_id, log_delta)| (*table_id, stripped_change_log_delta(log_delta)))
                .collect(),
            state_table_info_delta: delta.state_table_info_delta.clone(),
        }
    }
}

impl IncompleteHummockVersionDelta {
    /// Resulted `SStableInfo` is incompelte.
    pub fn to_protobuf(&self) -> PbHummockVersionDelta {
        PbHummockVersionDelta {
            id: self.id,
            prev_id: self.prev_id,
            group_deltas: self.group_deltas.clone(),
            max_committed_epoch: self.max_committed_epoch,
            safe_epoch: self.safe_epoch,
            trivial_move: self.trivial_move,
            new_table_watermarks: self
                .new_table_watermarks
                .iter()
                .map(|(table_id, watermarks)| (table_id.table_id, watermarks.to_protobuf()))
                .collect(),
            removed_table_ids: self
                .removed_table_ids
                .iter()
                .map(|table_id| table_id.table_id)
                .collect(),
            change_log_delta: self
                .change_log_delta
                .iter()
                .map(|(table_id, log_delta)| (table_id.table_id, log_delta.clone()))
                .collect(),
            state_table_info_delta: self
                .state_table_info_delta
                .iter()
                .map(|(table_id, delta)| (table_id.table_id, delta.clone()))
                .collect(),
        }
    }
}
