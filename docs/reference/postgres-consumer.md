# PostgreSQL Consumer Reference

`PostgresMessageConsumer` is the worker-side implementation for the queue
schema owned by `romanfedorskij/message-bus-async`. It implements
`MessageConsumerInterface` and `QueueLeaseControlInterface` through
`amphp/postgres`; no blocking PDO path exists.

## Claim Contract

`next(ConsumerOptions, Cancellation)` first recovers stale jobs for the selected
transport and queue, then claims one available `pending` row in a transaction:

1. filter by transport, queue, availability, and optional flow/binding filters;
2. order by priority descending, availability ascending, and queue ID ascending;
3. lock one row with `FOR UPDATE SKIP LOCKED`;
4. change it to `running`, increment attempts, and persist worker ID, lock time,
   heartbeat time, and start/update timestamps;
5. commit before returning `ReceivedQueueMessage`.

The returned value contains the queue ID, decoded versioned
`SerializedEnvelope`, immutable retry snapshot, current attempt number, and
lease owner. It never exposes a raw PostgreSQL row.

`ConsumerOptions` preserves the v6 defaults: timeout 5 seconds, limit 1, worker
ID `message-bus-worker`, and lock TTL 300 seconds. The current PostgreSQL claim
returns one message; timeout and multi-message limit remain runner concerns.

## Lease Invariant

Ack, retry, reject, cancel, and heartbeat update only a row matching all three
conditions:

- the received queue ID;
- `status = running`;
- `locked_by` equals the received lease owner.

Zero affected rows throw `QueueLeaseLost`. This prevents a slow or resumed
worker from committing a transition after stale recovery transferred the job to
another worker.

`isCancellationRequested()` uses the same guard. Producer-side
`requestCancellation()` only persists the flag; worker code must poll it and
persist `cancelled` when it cooperatively stops the handler.

## Durable Transitions

| Operation | From | To | Persisted behavior |
| --- | --- | --- | --- |
| `ack` | `running` | `succeeded` | Finish time set; lease cleared |
| `retry` | `running` | `pending` | Next availability and error set; lease cleared |
| exhausted `retry` | `running` | `failed` | Finish/error set; lease cleared |
| `reject` | `running` | `failed` | Finish/error set; lease cleared |
| `cancel` | `running` | `cancelled` | Finish/error set; lease cleared |
| `heartbeat` | `running` | `running` | Heartbeat/update timestamps refreshed |

Fixed and exponential delays preserve the v6 calculation and use the persisted
attempt number. Error details contain class, message, code, source location, and
a bounded stack trace. They must not contain application secrets; handlers are
responsible for using safe exception messages.

## Stale Recovery

`recoverStale()` selects `running` rows whose latest heartbeat/lock time is older
than the configured TTL. Selection uses `FOR UPDATE SKIP LOCKED`; all selected
rows transition in the same transaction. Recoverable rows return to `pending`
with their retry delay, while exhausted rows become `failed`.

The recovery call is cancellation-aware between rows. Cancellation rolls back
the complete recovery transaction. Like the core producer, AMPHP PostgreSQL 2.x
does not accept a cancellation token for an already-running SQL statement, so
the consumer observes cancellation only at safe boundaries and waits for an
in-flight statement to reach a definite result.

## Remaining Runtime Layer

This component does not execute handlers. The pending worker runner slice must:

- open and deterministically dispose one message scope per claimed job;
- deserialize the envelope through the core codec registry;
- propagate AMPHP cancellation into handler and nested bus execution;
- heartbeat long operations and poll the durable cancellation flag;
- choose ack/retry/reject/cancel from the handler outcome;
- stop gracefully without abandoning an in-process transition.
