// Copyright 2023 RisingWave Labs
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

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use risingwave_common::config::{
    extract_storage_memory_config, load_config, AsyncStackTraceOption, RwConfig,
};
use risingwave_common::monitor::connection::{RouterExt, TcpConfig};
use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
use risingwave_common::telemetry::manager::TelemetryManager;
use risingwave_common::telemetry::telemetry_env_enabled;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::resource_util;
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_common_service::metrics_manager::MetricsManager;
use risingwave_common_service::observer_manager::ObserverManager;
use risingwave_object_store::object::object_metrics::{self, GLOBAL_OBJECT_STORE_METRICS};
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::common::WorkerType;
use risingwave_pb::compactor::compactor_service_server::CompactorServiceServer;
use risingwave_pb::compactor::DispatchCompactionTaskRequest;
use risingwave_pb::hummock::hummock_manager_service_client::HummockManagerServiceClient;
use risingwave_pb::monitor_service::monitor_service_server::MonitorServiceServer;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::filter_key_extractor::{
    FilterKeyExtractorManager, RemoteTableAccessor, RpcFilterKeyExtractorManager,
    StaticFilterKeyExtractorManager,
};
use risingwave_storage::hummock::compactor::{CompactionExecutor, CompactorContext};
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::{
    HummockMemoryCollector, MemoryLimiter, SharedComapctorObjectIdManager, SstableObjectIdManager,
    SstableStore,
};
use risingwave_storage::monitor::{
    monitor_cache, GLOBAL_COMPACTOR_METRICS, GLOBAL_HUMMOCK_METRICS,
};
use risingwave_storage::opts::StorageOpts;
use tokio::sync::mpsc;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tracing::info;

use super::compactor_observer::observer_manager::CompactorObserverNode;
use crate::rpc::{CompactorServiceImpl, MonitorServiceImpl};
use crate::telemetry::CompactorTelemetryCreator;
use crate::CompactorOpts;

pub async fn prepare_start_parameters(
    config: RwConfig,

    storage_opts: Arc<StorageOpts>,
    object_metrics: Arc<object_metrics::ObjectStoreMetrics>,
    state_store_url: &str,
) -> (
    Arc<SstableStore>,
    Arc<MemoryLimiter>,
    Option<Arc<RwLock<await_tree::Registry<String>>>>,
) {
    let total_memory_available_bytes =
        (resource_util::memory::total_memory_available_bytes() as f64
            * config.storage.compactor_memory_available_proportion) as usize;
    let meta_cache_capacity_bytes = storage_opts.meta_cache_capacity_mb * (1 << 20);
    let compactor_memory_limit_bytes = match config.storage.compactor_memory_limit_mb {
        Some(compactor_memory_limit_mb) => compactor_memory_limit_mb as u64 * (1 << 20),
        None => (total_memory_available_bytes - meta_cache_capacity_bytes) as u64,
    };

    tracing::info!(
        "Compactor total_memory_available_bytes {} meta_cache_capacity_bytes {} compactor_memory_limit_bytes {} sstable_size_bytes {} block_size_bytes {}",
        total_memory_available_bytes, meta_cache_capacity_bytes, compactor_memory_limit_bytes,
        storage_opts.sstable_size_mb * (1 << 20),
        storage_opts.block_size_kb * (1 << 10),
    );

    // check memory config
    {
        // This is a similar logic to SstableBuilder memory detection, to ensure that we can find
        // configuration problems as quickly as possible
        let min_compactor_memory_limit_bytes = (storage_opts.sstable_size_mb * (1 << 20)
            + storage_opts.block_size_kb * (1 << 10))
            as u64;

        assert!(compactor_memory_limit_bytes > min_compactor_memory_limit_bytes * 2);
    }

    let mut object_store = parse_remote_object_store(
        state_store_url
            .strip_prefix("hummock+")
            .expect("object store must be hummock for compactor server"),
        object_metrics,
        "Hummock",
    )
    .await;
    object_store.set_opts(
        storage_opts.object_store_streaming_read_timeout_ms,
        storage_opts.object_store_streaming_upload_timeout_ms,
        storage_opts.object_store_read_timeout_ms,
        storage_opts.object_store_upload_timeout_ms,
    );
    let object_store = Arc::new(object_store);
    let sstable_store = Arc::new(SstableStore::for_compactor(
        object_store,
        storage_opts.data_directory.to_string(),
        1 << 20, // set 1MB memory to avoid panic.
        meta_cache_capacity_bytes,
    ));

    let memory_limiter = Arc::new(MemoryLimiter::new(compactor_memory_limit_bytes));
    let storage_memory_config = extract_storage_memory_config(&config);
    let memory_collector: Arc<HummockMemoryCollector> = Arc::new(HummockMemoryCollector::new(
        sstable_store.clone(),
        memory_limiter.clone(),
        storage_memory_config,
    ));

    monitor_cache(memory_collector);

    let await_tree_config = match &config.streaming.async_stack_trace {
        AsyncStackTraceOption::Off => None,
        c => await_tree::ConfigBuilder::default()
            .verbose(c.is_verbose().unwrap())
            .build()
            .ok(),
    };
    let await_tree_reg: Option<
        Arc<parking_lot::lock_api::RwLock<parking_lot::RawRwLock, await_tree::Registry<_>>>,
    > = await_tree_config.map(|c| Arc::new(RwLock::new(await_tree::Registry::new(c))));
    (sstable_store, memory_limiter, await_tree_reg)
}

/// Fetches and runs compaction tasks.
pub async fn compactor_serve(
    listen_addr: SocketAddr,
    advertise_addr: HostAddr,
    opts: CompactorOpts,
) -> (JoinHandle<()>, JoinHandle<()>, Sender<()>) {
    let config = load_config(&opts.config_path, &opts);
    info!("Starting compactor node",);
    info!("> config: {:?}", config);
    info!(
        "> debug assertions: {}",
        if cfg!(debug_assertions) { "on" } else { "off" }
    );
    info!("> version: {} ({})", RW_VERSION, GIT_SHA);

    // Register to the cluster.
    let (meta_client, system_params_reader) = MetaClient::register_new(
        &opts.meta_address,
        WorkerType::Compactor,
        &advertise_addr,
        Default::default(),
        &config.meta,
    )
    .await
    .unwrap();

    info!("Assigned compactor id {}", meta_client.worker_id());
    meta_client.activate(&advertise_addr).await.unwrap();

    // Boot compactor
    let object_metrics = Arc::new(GLOBAL_OBJECT_STORE_METRICS.clone());
    let hummock_metrics = Arc::new(GLOBAL_HUMMOCK_METRICS.clone());
    let compactor_metrics = Arc::new(GLOBAL_COMPACTOR_METRICS.clone());

    let hummock_meta_client = Arc::new(MonitoredHummockMetaClient::new(
        meta_client.clone(),
        hummock_metrics.clone(),
    ));

    let state_store_url = system_params_reader.state_store();

    let storage_memory_config = extract_storage_memory_config(&config);
    let storage_opts: Arc<StorageOpts> = Arc::new(StorageOpts::from((
        &config,
        &system_params_reader,
        &storage_memory_config,
    )));

    let (sstable_store, memory_limiter, await_tree_reg) = prepare_start_parameters(
        config.clone(),
        storage_opts.clone(),
        object_metrics,
        state_store_url,
    )
    .await;

    let telemetry_enabled = system_params_reader.telemetry_enabled();

    let filter_key_extractor_manager = Arc::new(RpcFilterKeyExtractorManager::new(Box::new(
        RemoteTableAccessor::new(meta_client.clone()),
    )));
    let system_params_manager = Arc::new(LocalSystemParamsManager::new(system_params_reader));
    let compactor_observer_node = CompactorObserverNode::new(
        filter_key_extractor_manager.clone(),
        system_params_manager.clone(),
    );
    let observer_manager =
        ObserverManager::new_with_meta_client(meta_client.clone(), compactor_observer_node).await;

    // use half of limit because any memory which would hold in meta-cache will be allocate by
    // limited at first.
    let observer_join_handle = observer_manager.start().await;

    let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
        hummock_meta_client.clone(),
        storage_opts.sstable_id_remote_fetch_number,
    ));
    let filter_key_extractor_manager = FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
        filter_key_extractor_manager.clone(),
    );
    let compactor_context = CompactorContext {
        storage_opts,
        sstable_store: sstable_store.clone(),
        compactor_metrics,
        is_share_buffer_compact: false,
        compaction_executor: Arc::new(CompactionExecutor::new(
            opts.compaction_worker_threads_number,
        )),
        memory_limiter,

        task_progress_manager: Default::default(),
        await_tree_reg: await_tree_reg.clone(),
        running_task_count: Arc::new(AtomicU32::new(0)),
    };
    let mut sub_tasks = vec![
        MetaClient::start_heartbeat_loop(
            meta_client.clone(),
            Duration::from_millis(config.server.heartbeat_interval_ms as u64),
            vec![sstable_object_id_manager.clone()],
        ),
        risingwave_storage::hummock::compactor::start_compactor(
            compactor_context.clone(),
            hummock_meta_client.clone(),
            sstable_object_id_manager.clone(),
            filter_key_extractor_manager.clone(),
        ),
    ];

    let telemetry_manager = TelemetryManager::new(
        system_params_manager.watch_params(),
        Arc::new(meta_client.clone()),
        Arc::new(CompactorTelemetryCreator::new()),
    );
    // if the toml config file or env variable disables telemetry, do not watch system params change
    // because if any of configs disable telemetry, we should never start it
    if config.server.telemetry_enabled && telemetry_env_enabled() {
        if telemetry_enabled {
            telemetry_manager.start_telemetry_reporting().await;
        }
        sub_tasks.push(telemetry_manager.watch_params_change());
    } else {
        tracing::info!("Telemetry didn't start due to config");
    }

    let compactor_srv = CompactorServiceImpl::default();
    let monitor_srv = MonitorServiceImpl::new(await_tree_reg);
    let (shutdown_send, mut shutdown_recv) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CompactorServiceServer::new(compactor_srv))
            .add_service(MonitorServiceServer::new(monitor_srv))
            .monitored_serve_with_shutdown(
                listen_addr,
                "grpc-compactor-node-service",
                TcpConfig {
                    tcp_nodelay: true,
                    keepalive_duration: None,
                },
                async move {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {},
                        _ = &mut shutdown_recv => {
                            for (join_handle, shutdown_sender) in sub_tasks {
                                if let Err(err) = shutdown_sender.send(()) {
                                    tracing::warn!("Failed to send shutdown: {:?}", err);
                                    continue;
                                }
                                if let Err(err) = join_handle.await {
                                    tracing::warn!("Failed to join shutdown: {:?}", err);
                                }
                            }
                        },
                    }
                },
            )
            .await
    });

    // Boot metrics service.
    if config.server.metrics_level > 0 {
        MetricsManager::boot_metrics_service(opts.prometheus_listener_addr.clone());
    }

    (join_handle, observer_join_handle, shutdown_send)
}

pub async fn shared_compactor_serve(
    listen_addr: SocketAddr,
    opts: CompactorOpts,
) -> (JoinHandle<()>, Sender<()>) {
    let endpoint: &'static str = Box::leak(opts.proxy_rpc_endpoint.clone().into_boxed_str());
    let channel = Channel::from_static(endpoint).connect().await.unwrap();

    let client: HummockManagerServiceClient<Channel> = HummockManagerServiceClient::new(channel);

    let config = load_config(&opts.config_path, &opts);
    info!("Starting Serverless compactor node",);
    info!("> config: {:?}", config);
    info!(
        "> debug assertions: {}",
        if cfg!(debug_assertions) { "on" } else { "off" }
    );
    info!("> version: {} ({})", RW_VERSION, GIT_SHA);
    let config = load_config(&opts.config_path, &opts);
    info!("Starting compactor node",);
    info!("> config: {:?}", config);
    info!(
        "> debug assertions: {}",
        if cfg!(debug_assertions) { "on" } else { "off" }
    );
    info!("> version: {} ({})", RW_VERSION, GIT_SHA);

    // Boot compactor
    let object_metrics = Arc::new(GLOBAL_OBJECT_STORE_METRICS.clone());
    let compactor_metrics = Arc::new(GLOBAL_COMPACTOR_METRICS.clone());

    let storage_memory_config = extract_storage_memory_config(&config);

    let storage_opts: Arc<StorageOpts> = Arc::new(StorageOpts::new(
        &config,
        &storage_memory_config,
        opts.parallel_compact_size_mb,
        opts.sstable_size_mb,
        opts.block_size_kb,
        opts.bloom_false_positive,
        &opts.data_directory,
        &opts.state_store_url,
    ));

    let (sstable_store, memory_limiter, await_tree_reg) = prepare_start_parameters(
        config.clone(),
        storage_opts.clone(),
        object_metrics,
        &opts.state_store_url,
    )
    .await;
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let compactor_srv: CompactorServiceImpl = CompactorServiceImpl::new(sender);

    let cloned_await_tree_reg = await_tree_reg.clone();

    let monitor_srv = MonitorServiceImpl::new(await_tree_reg);
    let (shutdown_send, mut shutdown_recv) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CompactorServiceServer::new(compactor_srv))
            .add_service(MonitorServiceServer::new(monitor_srv))
            .monitored_serve_with_shutdown(
                listen_addr,
                "grpc-compactor-node-service",
                TcpConfig {
                    tcp_nodelay: true,
                    keepalive_duration: None,
                },
                async move {
                    loop {
                        tokio::select! {
                            request = receiver.recv() => {
                                if let Some(request) = request {
                                    let DispatchCompactionTaskRequest {
                                        tables,
                                        output_object_ids,
                                        task: dispatch_task,
                                    } = request.into_inner();
                                    let id_to_tables = tables.into_iter().fold(HashMap::new(), |mut acc, table| {
                                        acc.insert(table.id, table);
                                        acc
                                    });
                                    let static_filter_key_extractor_manager: Arc<StaticFilterKeyExtractorManager> =
                                        Arc::new(StaticFilterKeyExtractorManager::new(id_to_tables));
                                    let filter_key_extractor_manager =
                                        FilterKeyExtractorManager::StaticFilterKeyExtractorManager(
                                            static_filter_key_extractor_manager,
                                        );
                                    let compactor_context = CompactorContext {
                                        storage_opts: storage_opts.clone(),
                                        sstable_store: sstable_store.clone(),
                                        compactor_metrics: compactor_metrics.clone(),
                                        is_share_buffer_compact: false,
                                        compaction_executor: Arc::new(CompactionExecutor::new(
                                            opts.compaction_worker_threads_number,
                                        )),
                                        memory_limiter: memory_limiter.clone(),
                                        task_progress_manager: Default::default(),
                                        await_tree_reg: cloned_await_tree_reg.clone(),
                                        running_task_count: Arc::new(AtomicU32::new(0)),
                                    };
                                    let mut output_object_ids_deque: VecDeque<_> = VecDeque::new();
                                    output_object_ids_deque.extend(output_object_ids);
                                    let shared_compactor_object_id_manager =
                                        SharedComapctorObjectIdManager::new(output_object_ids_deque);
                                    let (join_handle, shutdown_sender) =
                                        risingwave_storage::hummock::compactor::start_shared_compactor(
                                            client.clone(),
                                            dispatch_task.unwrap(),
                                            compactor_context,
                                            Box::new(shared_compactor_object_id_manager),
                                            filter_key_extractor_manager.clone()
                                        );
                                    tokio::select! {
                                        _ = tokio::signal::ctrl_c() => {},
                                        _ = &mut shutdown_recv => {
                                                if let Err(err) = shutdown_sender.send(()) {
                                                    tracing::warn!("Failed to send shutdown: {:?}", err);
                                                }
                                                if let Err(err) = join_handle.await {
                                                    tracing::warn!("Failed to join shutdown: {:?}", err);
                                                }
                                        },
                                    }
                                } else {
                                    continue;
                                }
                            }
                        };
                    }
                },
            )
            .await
    });

    // Boot metrics service.
    if config.server.metrics_level > 0 {
        MetricsManager::boot_metrics_service(opts.prometheus_listener_addr.clone());
    }

    (join_handle, shutdown_send)
}
