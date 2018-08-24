defmodule Vayne.Metric.Txy.Mysql do

  @behaviour Vayne.Task.Metric

  alias Vayne.Metric.Txy.Util

  @metric ~w(
    slow_queries max_connections select_scan select_count
    com_update com_delete com_insert com_replace
    queries threads_connected real_capacity capacity bytes_sent
    bytes_received qcache_use_rate qcache_hit_rate table_locks_waited
    created_tmp_tables innodb_cache_use_rate innodb_cache_hit_rate
    innodb_os_file_reads innodb_os_file_writes innodb_os_fsyncs
    key_cache_use_rate key_cache_hit_rate volume_rate query_rate qps tps
    cpu_use_rate memory_use key_write_requests key_writes com_commit
    handler_commit innodb_rows_read innodb_row_lock_time_avg threads_created
    opened_tables threads_running innodb_data_reads com_rollback
    key_blocks_unused innodb_data_writes innodb_buffer_pool_pages_free
    innodb_rows_inserted created_tmp_files innodb_data_read
    innodb_row_lock_waits innodb_buffer_pool_read_requests handler_rollback
    master_slave_sync_distance handler_read_rnd_next innodb_rows_updated
    innodb_rows_deleted innodb_buffer_pool_pages_total key_blocks_used
    innodb_data_written key_read_requests innodb_buffer_pool_reads
    created_tmp_disk_tables key_reads
  )

  @metric_MB ~w(real_capacity capacity memory_use master_slave_sync_distance)

  @doc """
  * `instanceId`: mongodb instanceId. Required.
  * `region`: db instance region. Required.
  * `secretId`: secretId for monitoring. Not required.
  * `secretKey`: secretKey for monitoring. Not required.
  """
  def init(params) do
    with {:ok, instanceId} <- Util.get_option(params, "instanceId"),
      {:ok, region} <- Util.get_option(params, "region"),
      {:ok, secret} <- Util.get_secret(params)
    do
      {:ok, {instanceId, region, secret}}
    else
      {:error, _} = e -> e
      error -> {:error, error}
    end
  end

  def run(stat, log_func) do
    metrics = Application.get_env(:vayne_metric_txy, :mysql_metric, @metric)
    ret = request_metric(metrics, stat, log_func)
    {:ok, ret}
  end

  def clean(_), do: :ok

  def request_metric(metrics, {instanceId, region, secret}, log_func) do
    Enum.reduce(metrics, %{}, fn (metric, acc) ->
      now = :os.system_time(:seconds)
      resp = "qce/cdb"
      |> Util.make_url(now, metric, {instanceId, region, secret})
      |> Util.request_url
      case resp do
        {:ok, nil} ->
          log_func.("get empty value. instance: #{instanceId}, metric: #{metric}")
          acc
        {:error, error} ->
          log_func.(error)
          acc
        {:ok, value} ->
          value = if metric in @metric_MB, do: value * 1024 * 1024, else: value
          Map.put(acc, metric, value)
      end
    end)
  end
end
