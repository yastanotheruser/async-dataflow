defmodule ChannelSenderEx.Core.ChannelSupervisorSyn do
  @moduledoc """
  Module to start supervised channels in a distributed way
  """

  use DynamicSupervisor

  import ChannelSenderEx.Core.Retry.ExponentialBackoff, only: [execute: 5]

  require Logger

  alias ChannelSenderEx.Core.Channel
  alias ChannelSenderEx.Utils.CustomTelemetry

  @scope :channels

  @max_retries 5
  @min_backoff 50
  @max_backoff 200

  def start_link(_) do
    res = DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
    Logger.info("Channel Supervisor started")
    res
  end

  def init(_) do
    :syn.add_node_to_scopes([@scope])
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @type channel_ref :: String.t()
  @type application :: String.t()
  @type user_ref :: String.t()
  @type meta :: list()
  @type channel_init_args :: {channel_ref(), application(), user_ref(), meta()}

  @spec start_channel(channel_init_args()) :: any()
  def start_channel(args) do
    Logger.debug(fn -> "Channel Supervisor, starting channel with args: #{inspect(args)}" end)

    case DynamicSupervisor.start_child(__MODULE__, {Channel, args}) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, reason} ->
        Logger.error(fn ->
          "Channel Supervisor, failed to register channel with args: #{inspect(args)}, reason: #{inspect(reason)}"
        end)

        {:error, reason}
    end
  end

  @spec register_channel(channel_init_args()) :: any()
  def register_channel(args = {_channel_ref, _application, _user_ref, _meta}) do
    with {:ok, pid} <- start_channel(args),
         :ok <- do_register(args, pid) do
      {:ok, pid}
    else
      {:error, reason} ->
        Logger.error(fn ->
          "Channel Supervisor, failed to register channel with args: #{inspect(args)}, reason: #{inspect(reason)}"
        end)

        {:error, reason}
    end
  end

  @spec start_channel_if_not_exists(channel_init_args()) :: any()
  def start_channel_if_not_exists(args = {channel_ref, _application, _user_ref, _meta}) do
    pid = whereis_channel(channel_ref)

    if pid == :undefined or not Channel.alive?(pid) do
      CustomTelemetry.execute_custom_event([:adf, :channel, :created_on_socket], %{count: 1})
      register_channel(args)
    else
      {:ok, pid}
    end
  end

  @spec register_channel_if_not_exists(channel_init_args()) :: any()
  def register_channel_if_not_exists(args = {channel_ref, _application, _user_ref, _meta}) do
    case :syn.lookup(@scope, channel_ref) do
      {pid, _meta} ->
        register_if_not_running(args, pid, self())

      :undefined ->
        pid = self()

        Logger.debug(fn ->
          "Channel Supervisor, channel #{channel_ref} not exists : nil self #{inspect(pid)}"
        end)

        do_register(args, pid)
        {:ok, pid}
    end
  end

  @spec whereis_channel(channel_ref()) :: pid() | :undefined
  def whereis_channel(channel_ref) do
    with {pid, _meta} <- :syn.lookup(@scope, channel_ref), do: pid
  end

  defp register_if_not_running(
         args = {channel_ref, _application, _user_ref, _meta},
         pid,
         self_pid
       ) do
    if Channel.alive?(pid) do
      Logger.debug(fn ->
        "Channel Supervisor, channel #{channel_ref} exists : #{inspect(pid)} self #{inspect(self_pid)}"
      end)

      {:ok, pid}
    else
      Logger.debug(fn ->
        "Channel Supervisor, channel #{channel_ref} not alive : #{inspect(pid)} self #{inspect(self_pid)}"
      end)

      do_register(args, self_pid)
      {:ok, self_pid}
    end
  end

  defp do_register(args = {channel_ref, _application, _user_ref, _meta}, pid) do
    execute(
      @min_backoff,
      @max_backoff,
      @max_retries,
      fn -> register_attempt(args, pid) end,
      fn ->
        Logger.warning("failed to register channel #{channel_ref} after #{@max_retries} attempts")
        :ok
      end
    )
  end

  defp register_attempt({channel_ref, application, user_ref, _meta}, pid) do
    with :ok <- :syn.register(@scope, channel_ref, pid),
         {^pid, _meta} <- :syn.lookup(@scope, channel_ref),
         app_group = {:app, application},
         :ok <- :syn.join(@scope, app_group, pid),
         {^pid, _meta} <- :syn.member(@scope, app_group, pid),
         user_group = {:user, user_ref},
         :ok <- :syn.join(@scope, user_group, pid),
         {^pid, _meta} <- :syn.member(@scope, user_group, pid) do
      :ok
    else
      {:error, error} ->
        Logger.error("channel #{channel_ref} register attempt failed - #{inspect(error)}")
        :retry

      :undefined ->
        Logger.error("channel #{channel_ref} failed to verify register")
        :retry
    end
  end

  @spec publish(kind :: :app | :user, name :: application() | user_ref(), message :: term) ::
          {:ok, recipient_count :: non_neg_integer}
  def publish(kind, name, message) do
    :syn.publish(@scope, {kind, name}, message)
  end
end
