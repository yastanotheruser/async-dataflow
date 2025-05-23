defmodule ChannelSenderEx.Core.ChannelSupervisorSyn do
  @moduledoc """
  Module to start supervised channels in a distributed way
  """

  use DynamicSupervisor

  require Logger

  alias ChannelSenderEx.Core.Channel
  alias ChannelSenderEx.Utils.CustomTelemetry

  @scope :channels

  @type channel_ref :: String.t()
  @type application :: String.t()
  @type user_ref :: String.t()
  @type meta :: term()
  @type channel_init_args :: {channel_ref(), application(), user_ref(), meta()}

  def start_link(_) do
    res = DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
    Logger.info("Channel Supervisor started")
    res
  end

  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

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
          "Channel Supervisor, failed to start channel with args: #{inspect(args)}, reason: #{inspect(reason)}"
        end)

        {:error, reason}
    end
  end

  @spec register_channel(Channel.Data.t(), meta :: term) :: :ok
  def register_channel(
        %Channel.Data{
          channel: channel_ref,
          application: application,
          user_ref: user_ref,
          meta: initial_meta
        },
        meta
      ) do
    pid = self()

    with :ok <- :syn.register(@scope, channel_ref, pid, meta),
         :ok <- :syn.join(@scope, {:app, application}, pid),
         :ok <- :syn.join(@scope, {:user, user_ref}, pid) do
      :ok
    else
      {:error, reason} ->
        Logger.error(fn ->
          args = {channel_ref, application, user_ref, initial_meta}
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
      start_channel(args)
    else
      {:ok, pid}
    end
  end

  @spec whereis_channel(channel_ref()) :: pid() | :undefined
  def whereis_channel(channel_ref) do
    case :syn.lookup(@scope, channel_ref) do
      {pid, _meta} -> pid
      :undefined -> :undefined
    end
  end

  @spec update_meta(channel_ref(), fun :: (pid(), term() -> term())) ::
          {:ok, {pid(), meta :: term()}}
  def update_meta(channel_ref, fun) when is_function(fun, 2) do
    :syn.update_registry(@scope, channel_ref, fun)
  end

  @spec publish(kind :: :app | :user, name :: application() | user_ref(), message :: term) ::
          {:ok, recipient_count :: non_neg_integer}
  def publish(kind, name, message) do
    :syn.publish(@scope, {kind, name}, message)
  end

  @spec resume_channel({state :: atom, Channel.Data.t()}) :: {:ok, pid} | {:error, term}
  def resume_channel(
        {_state,
         %Channel.Data{
           channel: channel_ref,
           application: application,
           user_ref: user_ref
         }} = info
      ) do
    case whereis_channel(channel_ref) do
      pid when is_pid(pid) ->
        {:error, {:already_started, pid}}

      :undefined ->
        start_channel({channel_ref, application, user_ref, {:failover, info}})
    end
  end
end
