# Message Bus Async Worker

Optional operational worker runtime for
`romanfedorskij/message-bus-async`.

> Status: pre-release `0.1.0`. PostgreSQL consumption and durable transitions
> are implemented, but the runner, per-job scope, control plane, and executable
> are not ready for application use.

## Responsibility Boundary

The worker package owns:

- durable PostgreSQL claim and ack/retry/reject/cancel transitions;
- lease-owner guarded heartbeat, cancellation polling, and stale recovery;
- consumer options and received-message contracts.

The package will also own the per-job lifecycle scope, cancellable runner,
worker control plane, Symfony Console executable, and optional process/signal
adapters. Those runtime layers are not implemented yet.

Producer, status, cancellation, common queue schema, message registry, and local
dispatch remain in the core package. The dependency direction is always worker
to core; the core package must not reference worker symbols.

## Platform

- PHP `8.4.12` or newer within the PHP 8 major line;
- `romanfedorskij/message-bus-async` `0.1.x`;
- `amphp/amp` `3.1.x` or a compatible 3.x release;
- `amphp/postgres` `2.2.1` or a compatible 2.x release, with `ext-pgsql` or
  `ext-pq` available to the worker process;
- `psr/clock` `1.0.x`.

Console and process dependencies will be declared only with the implementation
slice that uses them.

## PostgreSQL Consumer

`PostgresMessageConsumer` preserves the v6 consumer operations while replacing
PDO with `amphp/postgres` and adding AMPHP cancellation at safe I/O boundaries.
It claims one available job through `FOR UPDATE SKIP LOCKED`, persists the lease
before returning the message, and guards every transition by job ID, `running`
state, and lease owner. A worker that lost its lease therefore cannot ack or
overwrite the job after another worker recovered it.

Retry uses the policy snapshot stored by the producer. Exhausted jobs become
`failed`; stale leases return to `pending` or become `failed` when the attempt
budget is exhausted. Stale selection and transitions run in one transaction
with row locks, so parallel recovery processes do not recover the same job.
See the [PostgreSQL consumer reference](docs/reference/postgres-consumer.md) for
the exact contracts and lifecycle limits.

## Development

The source workspace currently expects the core repository in the sibling
`../MessageBusAsync` directory. Composer links it as version `0.1.0`; package
consumers will resolve the published `^0.1` dependency normally.

Run the fast gates with:

```bash
composer validate --strict --no-check-lock
composer analyse
composer test
```

Run the real PostgreSQL transition gate against a disposable database whose
user may create and drop tables:

```bash
MESSAGE_BUS_TEST_POSTGRES_CONFIG='host=127.0.0.1 port=5432 user=test password=test db=test' \
    composer test:integration
```
