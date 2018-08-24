defmodule Vayne.Metric.Txy.Mongodb do

  @behaviour Vayne.Task.Metric

  alias Vayne.Metric.Txy.Util

  @metric_instance ~w(
    inserts reads updates deletes counts aggregates cluster_diskusage success
    delay_10 delay_50 delay_100 connper
  )

  @metric_replica ~w(replica_diskusage slavedelay)

  @metric_node ~w( cpuusage memusage qr qw conn netin netout)

  @metric_MB ~w(netin netout)

  @doc """

  * `instanceId`: mongodb instanceId. Required.
  * `region`: db instance region. Required.
  * `replica_count`: mongodb replica count. Not required.
  * `node_num`: mongodb node num. Not required.
  * `secretId`: secretId for monitoring. Not required.
  * `secretKey`: secretKey for monitoring. Not required.
  """

  def init(params) do
    with {:ok, instanceId} <- Util.get_option(params, "instanceId"),
      {:ok, region} <- Util.get_option(params, "region"),
      {:ok, secret} <- Util.get_secret(params),
      replica_count <- Map.get(params, "replica_count"),
      node_num      <- Map.get(params, "node_num")
    do
      {:ok, {instanceId, replica_count, node_num, region, secret}}
    else
      {:error, _} = e -> e
      error -> {:error, error}
    end
  end

  def run({instanceId, replica_count, node_num, region, secret}, log_func) do

    ret = []

    metric_instance = request_metric(@metric_instance, {instanceId, region, secret}, log_func)

    ret = [{%{}, metric_instance} | ret]

    ret = if replica_count do
      replica_metrics = Enum.map(0..(replica_count - 1), fn replica ->
        replica_instanceId = "#{instanceId}_#{replica}"
        metric_replica = request_metric(
          @metric_replica,
          {replica_instanceId, region, secret}, log_func
        )
        {%{"replica" => replica}, metric_replica}
      end)
      replica_metrics ++ ret
    else
      ret
    end

    ret = if is_integer(replica_count) and is_integer(node_num) do
      nodes = Enum.map(0..(node_num - 2), &("node-slave#{&1}")) ++ ["node-primary"]

      node_metrics = for replica <- 0 .. (replica_count - 1), node <- nodes
      do
        node_instanceId = "#{instanceId}_#{replica}-#{node}"
        metric = request_metric(
          @metric_node,
          {node_instanceId, region, secret}, log_func
        )
        {%{"replica" => replica, "node" => node}, metric}
      end
      node_metrics ++ ret
    else
      ret
    end

    {:ok, ret}
  end

  def clean(_), do: :ok

  def request_metric(metrics, {instanceId, region, secret}, log_func) do
    Enum.reduce(metrics, %{}, fn (metric, acc) ->
      now = :os.system_time(:seconds)
      resp = "qce/cmongo"
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
