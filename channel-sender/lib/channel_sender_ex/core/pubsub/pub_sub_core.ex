defmodule ChannelSenderEx.Core.PubSub.PubSubCore do
  @moduledoc """
  Handles channel delivery and discovery logic
  """
  require Logger

  alias ChannelSenderEx.Core.Channel
  alias ChannelSenderEx.Core.ChannelSupervisorSyn, as: ChannelSupervisor
  alias ChannelSenderEx.Core.ProtocolMessage
  alias ChannelSenderEx.Utils.CustomTelemetry
  import ChannelSenderEx.Core.Retry.ExponentialBackoff, only: [execute: 5]

  @type channel_ref() :: String.t()
  @type app_ref() :: String.t()
  @type user_ref() :: String.t()

  @max_retries 10
  @min_backoff 50
  @max_backoff 2000

  @doc """
  Delivers a message to a single channel associated with the given channel reference.
  If the channel is not found, the message is retried up to @max_retries times with exponential backoff.
  """
  @spec deliver_to_channel(channel_ref(), ProtocolMessage.t()) :: any()
  def deliver_to_channel(channel_ref, message) do
    action_fn = fn _ -> do_deliver_to_channel(channel_ref, message) end
    execute(@min_backoff, @max_backoff, @max_retries, action_fn, fn ->
      CustomTelemetry.execute_custom_event([:adf, :message, :nodelivered], %{count: 1})
      raise("No channel found")
    end)
  rescue
    e ->
      Logger.warning("Could not deliver message after #{@max_retries} retries, to channel: \"#{channel_ref}\". Cause: #{inspect(e)}")
      :error
  end

  @doc """
  Delivers a message to all channels associated with the given application reference. The message is delivered to each channel in a separate process.
  No retries are performed since the message is delivered to existing and queriyable channels at the given time.
  """
  @spec deliver_to_app_channels(app_ref(), ProtocolMessage.t()) ::
          {:ok, recipient_count :: non_neg_integer}
  def deliver_to_app_channels(app_ref, message) do
    ChannelSupervisor.publish(:app, app_ref, cast(message))
  end

  @doc """
  Delivers a message to all channels associated with the given user reference. The message is delivered to each channel in a separate process.
  No retries are performed since the message is delivered to existing and queriyable channels at the given time.
  """
  @spec deliver_to_user_channels(user_ref(), ProtocolMessage.t()) ::
          {:ok, recipient_count :: non_neg_integer}
  def deliver_to_user_channels(user_ref, message) do
    ChannelSupervisor.publish(:user, user_ref, cast(message))
  end

  @compile {:inline, cast: 1}
  defp cast(message), do: {:"$gen_cast", message}

  defp do_deliver_to_channel(channel_ref, message) do
    case ChannelSupervisor.whereis_channel(channel_ref) do
      pid when is_pid(pid) -> Channel.deliver_message(pid, message)
      :undefined ->
        :retry
    end
  end

  def delete_channel(channel_ref) do
    action_fn = fn _ -> do_delete_channel(channel_ref) end
    execute(@min_backoff, @max_backoff, @max_retries, action_fn, fn ->
      Logger.warning("Could not delete channel #{channel_ref} after #{@max_retries} retries")
      :ok
    end)
  end

  def do_delete_channel(channel_ref) do
    case  ChannelSupervisor.whereis_channel(channel_ref) do
      pid when is_pid(pid) -> Channel.stop(pid)
      :undefined ->
        :retry
    end
  end
end
