# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-11-07

First public release.

- Added AMPHP PostgreSQL claim, heartbeat, ack, retry, reject, cancellation,
  and stale-recovery transitions with lease-owner guards.
- Added a cooperative runner with per-job scopes and graceful cancellation.
- Added exact-binding execution through the core message bus.
- Added durable worker registry, desired state, commands, acknowledgements, and
  status storage.
- Added optional PCNTL signal integration and Thesis DIC composition.
- Added `message-bus-worker` commands for run, status, control actions, and
  PostgreSQL control-schema generation.

[0.1.0]: https://github.com/wolfcharaa/message-bus-async-worker/releases/tag/v0.1.0
