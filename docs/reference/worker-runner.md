# Worker Runner

`QueueWorkerRunner` — операционный AMPHP loop вокруг
`MessageConsumerInterface`, `QueueLeaseControlInterface` и отдельного
`ServiceScopeInterface` на job. Он не использует blocking sleep и не зависит
от Symfony Console, PCNTL или process-global mutable state.

## Job Scope

Для каждого `ReceivedQueueMessage` runner открывает новый scope через core
`ServiceScopeFactoryInterface`, разрешает из него `QueueWorkerInterface` и
вызывает:

```php
public function handle(
    ReceivedQueueMessage $message,
    Amp\Cancellation $cancellation,
): void;
```

Встроенный `MessageBusQueueWorker` десериализует core
`SerializedEnvelope`, применяет authoritative `QueueMessage::bindingId` и
вызывает `BindingEnvelopeExecutorInterface` с текущими scope resolver и
cancellation. Core `MessageBus` принудительно выполняет один binding локально,
не вызывая queue strategy повторно. Scope закрывается один раз и получает
исходную handler error, если она была.

## Параллельный Lease Monitor

Пока cooperative handler ожидает I/O, AMPHP monitor:

- обновляет heartbeat с `heartbeatIntervalMilliseconds`;
- опрашивает durable cancellation с
  `cancellationPollIntervalMilliseconds`;
- замечает process cancellation, не бросая job сразу;
- отменяет handler после `gracefulShutdownTimeoutSeconds`.

`QueueLeaseLost` отменяет handler и запрещает этому worker позднее записать
ack/retry/reject. Durable cancellation использует причину
`QueueCancellationRequested`, истечение grace — `GracefulShutdownExpired`
и outcome unknown без автоматического retry. `maxJobRuntimeSeconds` задаёт
конечный cooperative deadline каждой job.

Monitoring кооперативен. CPU-bound handler без suspension не позволяет event
loop обновить heartbeat или заметить cancellation. Такую работу нужно дробить
с suspension points или переносить в process adapter.

## Durable Outcome

| Результат | Transition consumer |
| --- | --- |
| Handler и scope завершились успешно | `ack` |
| Durable cancellation request | `cancel` |
| Timeout, control kill или другая cooperative cancellation | `reject` с `QueueOutcomeUnknown` |
| Ошибка cleanup scope | `reject` с `QueueOutcomeUnknown`, stop admission и recycle exit code |
| `NonRetryableMessageExceptionInterface` | `reject` |
| Другая ошибка, attempts остались | `retry` |
| Другая ошибка, attempts исчерпаны | `reject` |
| Lease потерян | stale worker ничего не записывает |

Transitions выполняются с независимым `NullCancellation`. Process stop может
отменить handler после grace period, но не прерывает запись durable outcome.
PostgreSQL lease guards остаются финальной проверкой ownership.

`QueueWorkerRunResult` содержит handled, succeeded, retried, rejected,
cancelled и lease-lost counters.

## Ограничения Loop

`QueueWorkerRunnerOptions` поддерживает:

- максимальное число messages, runtime и idle duration;
- stop-on-empty и AMPHP idle delay;
- process memory ceiling;
- heartbeat и cancellation-poll intervals;
- graceful shutdown timeout и конечный runtime одной job;
- cadence stale recovery; каждый проход обрабатывает bounded batch из
  `ConsumerOptions`.

Cancellation в `run()` — запрос остановки. До claim он сразу завершает loop.
Во время job прекращает будущие claims и запускает grace window; текущий handler
может успеть завершиться и получить ack.

Optional `PcntlSignalCancellation` переводит `SIGTERM`, `SIGINT` и
`SIGQUIT` в cancellation и восстанавливает предыдущие handlers. Сам runner
остаётся независимым от Console и signal state.

## Worker Control Runtime

Optional `WorkerControlRuntime` включает control state machine. Runner
регистрирует single-mode worker, опрашивает commands/desired state до claims и
во время job, подтверждает команды, пишет heartbeat и финальное состояние.

Pause/resume влияют на будущие claims. Drain, stop и restart дают текущей job
закончиться. Kill cooperative-отменяет handler и сохраняет outcome unknown без
автоматического business retry. Restart возвращает через
`QueueWorkerRunResult::exitCode` код для supervisor.

Подробности — в [worker control](worker-control.md).
