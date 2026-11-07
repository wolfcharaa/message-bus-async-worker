# Message Bus Async Worker

Optional AMPHP worker runtime and Symfony Console CLI for
[`romanfedorskij/message-bus-async`](https://github.com/wolfcharaa/message-bus-async).
It consumes durable PostgreSQL jobs, opens one application service scope per
job, executes the exact queued binding, and persists guarded outcomes.

The first stable package line is `0.1.x` and requires a compatible core `^0.1`.

## What It Provides

| Capability | Main type or command |
| --- | --- |
| PostgreSQL claim and guarded transitions | `PostgresMessageConsumer` |
| Cooperative AMPHP consumer loop | `QueueWorkerRunner` |
| Exact queued-binding execution | `MessageBusQueueWorker` |
| Per-job service scopes | `WorkerModule` or application scope factory |
| Durable worker control and status | `PostgresWorkerControlGateway` |
| Process cancellation | optional `PcntlSignalCancellation` |
| Operational CLI | `vendor/bin/message-bus-worker` |

The core package remains responsible for dispatch, envelope codecs, queue
enqueue/status/cancel, and the shared queue schema. Dependency direction is
always worker to core.

## Requirements

- PHP `8.4.12` or newer within the PHP 8 line;
- `romanfedorskij/message-bus-async ^0.1`;
- AMPHP 3 and `amphp/postgres` 2;
- `ext-pgsql` or `ext-pq` for PostgreSQL;
- Symfony Console 7.4 or 8.1.

`ext-pcntl` and Thesis DIC are optional. Without PCNTL, shutdown must be
requested through worker control or another host adapter.

## Installation

Until both packages are registered in Packagist, configure their GitHub VCS
repositories explicitly:

```shell
composer config repositories.message-bus-async vcs \
  https://github.com/wolfcharaa/message-bus-async.git
composer config repositories.message-bus-async-worker vcs \
  https://github.com/wolfcharaa/message-bus-async-worker.git
composer require romanfedorskij/message-bus-async-worker:^0.1
```

Composer installs the executable as `vendor/bin/message-bus-worker`.

## First Worker

The host application must provide a bootstrap returning either a direct
`WorkerRuntime` or the recommended `WorkerRuntimeProviderInterface`. The
provider keeps pools, clients, and container scopes alive for the whole CLI
operation and disposes them afterward.

```shell
vendor/bin/message-bus-worker worker:run \
  --bootstrap=config/message-bus-worker.php \
  --transport=postgres \
  --queue=addresses
```

The package does not discover an application container and does not create its
schema on startup. Generate reference DDL with
`worker:schema:postgres`, then apply it through the host application's versioned
migrations before starting workers.

The complete direct and managed bootstrap examples, CLI options, status, and
control commands are in the [Worker CLI guide](docs/guides/worker-cli.md).

## Execution Model

1. The consumer claims one available row with a lease.
2. The runner opens a fresh job scope and resolves the queue worker.
3. A concurrent monitor sends heartbeats and observes cancellation/control.
4. The exact binding stored by the producer executes locally; it is not queued
   a second time.
5. The consumer acknowledges, retries, rejects, or cancels with lease-owner
   guards, then disposes the job scope.

Delivery is at-least-once. A crash or stale lease can execute a message again,
so state-changing handlers must be idempotent. CPU-bound handlers must expose
cooperative suspension points or run behind a separate process adapter.

## Process Boundary

One CLI process runs one cooperative worker. Multi-process scaling, restart on
exit code `75`, forced termination, and resource ceilings belong to systemd,
Supervisor, Kubernetes, Docker, or another external supervisor. The package has
no hidden fork pool.

## Documentation

- [Worker CLI guide](docs/guides/worker-cli.md) — bootstrap, run, status,
  control, schema generation, and exit behavior.
- [PostgreSQL consumer](docs/reference/postgres-consumer.md) — claims, leases,
  transitions, retry, and stale recovery.
- [Worker runner](docs/reference/worker-runner.md) — scopes, heartbeat,
  cancellation, outcome mapping, and limits.
- [Worker control](docs/reference/worker-control.md) — desired state, commands,
  targeting, acknowledgements, and storage ownership.
- [Core PostgreSQL queue](https://github.com/wolfcharaa/message-bus-async/blob/v0.1.0/docs/reference/postgres-queue.md)
  — producer-side schema and delivery contract.

## Development

The source workspace expects the core checkout in sibling directory
`../MessageBusAsync`; Composer links it as `0.1.0` for development.

```shell
composer validate --strict
composer analyse
composer test
```

The optional integration suite requires a disposable PostgreSQL database whose
user can create and drop tables:

```shell
MESSAGE_BUS_TEST_POSTGRES_CONFIG='host=127.0.0.1 port=5432 user=test password=test db=test' \
  composer test:integration
```

## License

[MIT](LICENSE)
