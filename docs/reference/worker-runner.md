# Worker Runner Reference

`QueueWorkerRunner` is the operational AMPHP loop around
`MessageConsumerInterface`, `QueueLeaseControlInterface`, and a per-job
`ServiceScopeInterface`. It performs no blocking sleep and has no dependency on
Symfony Console, PCNTL, or process-global mutable state.

## Job scope

Every claimed `ReceivedQueueMessage` opens a fresh scope through the core
`ServiceScopeFactoryInterface`. The runner resolves
`QueueWorkerInterface` from that scope and invokes:

```php
public function handle(
    ReceivedQueueMessage $message,
    Amp\Cancellation $cancellation,
): void;
```

The adapter is responsible for deserializing the embedded core
`SerializedEnvelope` and executing exactly `QueueMessage::bindingId`. This
keeps worker orchestration independent from an application container while
allowing Thesis DIC or another lifecycle adapter to create one message scope.
The scope is disposed once with the original handler error, if any.

## Concurrent lease monitor

While cooperative handler I/O is suspended, an AMPHP monitor:

- heartbeats the current lease at `heartbeatIntervalMilliseconds`;
- polls the durable cancellation flag at
  `cancellationPollIntervalMilliseconds`;
- observes process cancellation without immediately abandoning the job;
- cancels the handler after `gracefulShutdownTimeoutSeconds` expires.

`QueueLeaseLost` cancels the handler and prevents any later ack/retry/reject
write by this worker. Durable cancellation uses `QueueCancellationRequested` as
the cancellation cause. Grace-period expiry uses `GracefulShutdownExpired` and
follows the ordinary retry budget.

Monitoring is cooperative: a CPU-bound handler that never suspends prevents the
event loop from sending heartbeats or observing cancellation. Such work must be
chunked with suspension points or moved to a process adapter.

## Durable outcome mapping

| Outcome | Consumer transition |
| --- | --- |
| Handler and scope complete successfully | `ack` |
| Durable cancellation request | `cancel` |
| `NonRetryableMessageExceptionInterface` | `reject` |
| Other failure with attempts remaining | `retry` |
| Other failure with exhausted attempts | `reject` |
| Lease lost | No transition from the stale worker |

Transitions run with an independent `NullCancellation`. A process stop may
cancel handler work after the grace period, but it does not interrupt the
durable transition that records the outcome. PostgreSQL lease guards remain the
final authority if ownership changed concurrently.

`QueueWorkerRunResult` reports handled, succeeded, retried, rejected, cancelled,
and lease-lost counts.

## Loop limits

`QueueWorkerRunnerOptions` supports:

- maximum messages, process runtime, and idle duration;
- stop-on-empty behavior and AMPHP idle delay;
- a process memory ceiling;
- heartbeat and cancellation-poll intervals;
- graceful shutdown timeout.

The cancellation passed to `run()` is a stop request. Before a claim it ends the
loop immediately. During a job it stops future claims and starts the grace
window; the current handler may still finish and be acknowledged.

Signal and console adapters should translate their stop request into an AMPHP
`DeferredCancellation`. They remain separate optional process concerns and are
not part of this runner slice.
