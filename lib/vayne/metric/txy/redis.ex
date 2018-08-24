defmodule Vayne.Metric.Txy.Redis do

  @behaviour Vayne.Task.Metric

  alias Vayne.Metric.Txy.Util

  @metric ~w(
    cache_hit_ratio cmdstat_get cmdstat_getbit cmdstat_getrange cmdstat_hget
    cmdstat_hgetall cmdstat_hmget cmdstat_hmset cmdstat_hset cmdstat_hsetnx
    cmdstat_lset cmdstat_mget cmdstat_mset cmdstat_msetnx cmdstat_set
    cmdstat_setbit cmdstat_setex cmdstat_setnx cmdstat_setrange qps
    connections cpu_us in_flow keys out_flow stat_get stat_set storage storage_us
  )

  @metric_MB ~w(
    in_flow
    out_flow
  )

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

    metrics = Application.get_env(:vayne_metric_txy, :redis_metric, @metric)
    ret = request_metric(metrics, stat, log_func)
    {:ok, ret}
  end

  def clean(_), do: :ok

  def request_metric(metrics, {instanceId, region, secret}, log_func) do
    Enum.reduce(metrics, %{}, fn (metric, acc) ->
      now = :os.system_time(:seconds)
      resp = "qce/redis"
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
