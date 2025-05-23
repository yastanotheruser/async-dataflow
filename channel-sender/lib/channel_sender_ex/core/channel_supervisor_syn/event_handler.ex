defmodule ChannelSenderEx.Core.ChannelSupervisorSyn.EventHandler do
  @behaviour :syn_event_handler

  alias ChannelSenderEx.Core.ChannelSupervisorSyn

  @impl :syn_event_handler
  def on_process_unregistered(
        :channels,
        name,
        _pid,
        meta,
        {:syn_remote_scope_node_down, :channels, _node}
      ) do
    Task.Supervisor.start_child(
      __MODULE__,
      fn -> maybe_resume_channel(name, meta) end,
      restart: :transient
    )
  end

  def on_process_unregistered(_, _, _, _, _), do: :ok

  defp maybe_resume_channel(name, meta) do
    self = node()
    nodes = Enum.sort([self | Node.list()])
    node_count = Enum.count(nodes)
    self_index = Enum.find_index(nodes, &(&1 == self))

    if :erlang.phash2(name, node_count) == self_index do
      ChannelSupervisorSyn.resume_channel(dbg(meta))
    end
  end

  def child_spec(opts) do
    opts = Keyword.put(opts, :name, __MODULE__)
    Task.Supervisor.child_spec(opts)
  end
end
