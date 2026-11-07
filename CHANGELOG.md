# История Изменений

Здесь фиксируются значимые изменения проекта.

## [0.1.0] - 2026-11-07

Первый публичный release.

- Добавлены AMPHP PostgreSQL claim, heartbeat, ack, retry, reject, cancellation
  и stale-recovery transitions с lease-owner guards.
- Добавлен cooperative runner со scope на job и graceful cancellation.
- Добавлено exact-binding execution через core message bus.
- Добавлены durable worker registry, desired state, commands, acknowledgements
  и status storage.
- Добавлены optional PCNTL signal integration и Thesis DIC composition.
- Добавлены команды `message-bus-worker` для run, status, control actions и
  генерации PostgreSQL control schema.

[0.1.0]: https://github.com/wolfcharaa/message-bus-async-worker/releases/tag/v0.1.0
