import Config

config :channel_sender_ex,
  app_repo: ChannelSenderEx.Repository.ApplicationRepo,
  channel_shutdown_tolerance: 10_000,
  min_disconnection_tolerance: 50,
  socket_event_bus: ChannelSenderEx.Core.PubSub.SocketEventBus

config :syn,
  scopes: [:channels],
  event_handler: ChannelSenderEx.Core.ChannelSupervisorSyn.EventHandler,
  strict_mode: true

import_config "#{Mix.env()}.exs"
