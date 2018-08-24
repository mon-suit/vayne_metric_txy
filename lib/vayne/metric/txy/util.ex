defmodule Vayne.Metric.Txy.Util do

  def get_option(params, key) do
    case Map.fetch(params, key) do
      {:ok, _} = v -> v
      _ -> {:error, "#{key} is missing"}
    end
  end

  def get_secret(params) do

    env_secretId = Application.get_env(:vayne_metric_txy, :secretId)
    env_secretKey = Application.get_env(:vayne_metric_txy, :secretKey)

    cond do
      Enum.all?(~w(secretId secretKey), &(Map.has_key?(params, &1))) ->
        {:ok, {params["secretId"], params["secretKey"]}}
      Enum.all?([env_secretId, env_secretKey], &(not is_nil(&1))) ->
        {:ok, {env_secretId, env_secretKey}}
      true ->
        {:error, "secretId or secretKey is missing"}
    end
  end

  def sign_txy(hash, secretKey) do
    param = hash |> Map.keys |> Enum.sort |> Enum.map(fn k -> "#{k}=#{hash[k]}" end) |> Enum.join("&")
    c_url = "GETmonitor.api.qcloud.com/v2/index.php?" <> param
    signature = :crypto.hmac(:sha256, secretKey, c_url) |> Base.encode64

    hash = Map.put(hash, "Signature", signature)
    param = hash |> Map.keys |> Enum.sort
      |> Enum.map(fn k -> "#{k}=#{hash[k]|>to_string|>URI.encode_www_form}" end) |> Enum.join("&")
    "https://monitor.api.qcloud.com/v2/index.php?" <> param
  end

  @before_minutes -5
  def make_url(namespace, now, metric, {instanceId, region, {secretId, secretKey}}) do
    time      = now |> Timex.from_unix |> Timex.to_datetime("Asia/Shanghai")
    startTime = time |> Timex.shift(minutes: @before_minutes) |> Timex.format!("%F %T", :strftime)
    endTime   = time |> Timex.format!("%F %T", :strftime)

    %{
      "Action" => "GetMonitorData",
      "SecretId" => secretId,
      "Region" => region,
      "Timestamp" => now,
      "Nonce" => :rand.uniform(999999),
      "SignatureMethod" => "HmacSHA256",
      #"namespace" => "qce/cdb",
      "metricName" => metric,
      "dimensions.0.value" => instanceId,
      "period" => 60,
      "startTime" => startTime,
      "endTime" => endTime,
    }
    |> Map.merge(params_namespace(namespace))
    |> sign_txy(secretKey)
  end

  defp params_namespace("qce/cmongo") do
    %{
      "namespace" => "qce/cmongo",
      "dimensions.0.name" => "target",
    }
  end

  defp params_namespace("qce/redis") do
    %{
      "namespace" => "qce/redis",
      "dimensions.0.name" => "redis_uuid",
    }
  end

  defp params_namespace("qce/cdb") do
    %{
      "namespace" => "qce/cdb",
      "dimensions.0.name" => "uInstanceId",
    }
  end

  defp params_namespace(namespace), do: raise "not support #{namespace}"

  def request_url(url) do
    {:ok, worker_pid} = HTTPotion.spawn_worker_process(url)
    response = HTTPotion.get(url, timeout: :timer.seconds(10), direct: worker_pid)

    case response do
      %{body: body} ->
         case Poison.decode!(body) do
           %{"code" => 0, "codeDesc" => "Success", "dataPoints" => points} ->
             point = points |> Enum.filter(fn x -> not is_nil(x) end) |> List.last
             {:ok, point}
           %{"message" => error} ->
             {:error, error}
           error ->
             {:error, error}
         end
      error -> {:error, error}
    end
  end

end
