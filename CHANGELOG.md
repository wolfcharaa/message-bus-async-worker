# История Изменений

Здесь фиксируются значимые изменения проекта.

## [0.1.1] - 2026-11-09

- Lease transitions теперь fenced номером attempt даже при повторном
  использовании одного worker ID.
- Stale recovery отделён от claim path, ограничен batch size и запускается
  runner по отдельному cadence.
- Добавлен конечный runtime одной job; timeout, control kill и ошибки cleanup
  сохраняются как outcome unknown без автоматического retry.
- После ошибки cleanup runner прекращает admission и возвращает recycle exit
  code для supervisor.

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

[0.1.1]: https://github.com/wolfcharaa/message-bus-async-worker/releases/tag/v0.1.1
[0.1.0]: https://github.com/wolfcharaa/message-bus-async-worker/releases/tag/v0.1.0
