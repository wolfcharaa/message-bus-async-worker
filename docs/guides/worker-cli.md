# Worker CLI Guide

The package installs `vendor/bin/message-bus-worker` as the application root
for queue workers and worker-control operations. It uses Symfony Console but
does not discover or import an application container. The host application
must provide an explicit PHP bootstrap returning `WorkerRuntime`.

## Bootstrap

Create an application-owned file such as `config/message-bus-worker.php`:

```php
<?php

declare(strict_types=1);

use Fedor\MessageBus\Worker\Runtime\WorkerRuntime;

require __DIR__ . '/../bootstrap.php';

return new WorkerRuntime(
    runner: createQueueWorkerRunner(),
    control: createWorkerControlRuntime(),
);
```

`createQueueWorkerRunner()` and `createWorkerControlRuntime()` represent the
host application's composition functions. The runner is required. Control is
optional for queue-only execution, but all `worker:control`, action, and status
commands fail clearly when it is absent. When control is supplied, construct
the runner with the same `WorkerControlRuntime` instance so CLI commands and the
running worker share one control plane.

The resolver checks sources in this order:

1. `--bootstrap`;
2. `MESSAGE_BUS_WORKER_BOOTSTRAP`;
3. `message-bus-worker.php`, `config/message-bus-worker.php`, then
   `config/message_bus_worker.php` in the current working directory.

The returned value must be exactly a `WorkerRuntime`. PSR-11 and framework
container adaptation belongs in the application bootstrap, not this package.

## Run a worker

```bash
vendor/bin/message-bus-worker worker:run \
    --bootstrap=config/message-bus-worker.php \
    --transport=postgres \
    --queue=addresses \
    --worker-id=address-importer \
    --worker-name=address-importer-1 \
    --worker-group=imports \
    --flow=import \
    --binding-pattern='address.*'
```

The run command maps CLI values directly to `ConsumerOptions` and
`QueueWorkerRunnerOptions`. It supports consumer timeout/limit/lock TTL,
flow/binding filters, maximum messages/runtime/idle time, idle delay, memory
limit in bytes, heartbeat/cancellation/control polling intervals, graceful
shutdown timeout, and stop-on-empty behavior. Use `worker:run --help` for the
complete option list and defaults.

On exit it prints one machine-readable summary:

```text
handled=10 succeeded=8 retried=1 rejected=0 cancelled=1 lease_lost=0
```

Normal completion returns `0`. A durable restart command returns
`--restart-exit-code`, default `75`, so a process supervisor can replace the
process. Fatal bootstrap, configuration, storage, or handler failures are
reported by Symfony Console and return a non-zero code.

When `ext-pcntl` is loaded, `SIGTERM`, `SIGINT`, and `SIGQUIT` request the same
cooperative shutdown as an AMPHP cancellation: no new job is claimed, the
current job receives its configured grace period, and its durable transition
finishes before exit. Previous signal handlers are restored after the command.
This is not a hard-kill mechanism and cannot interrupt CPU-bound code that never
suspends.

## Control workers

Send a command through the general entrypoint:

```bash
vendor/bin/message-bus-worker worker:control \
    --bootstrap=config/message-bus-worker.php \
    --command=drain \
    --group=imports \
    --reason='deployment'
```

Or use the action aliases `worker:pause`, `worker:resume`, `worker:drain`,
`worker:stop`, `worker:kill`, and `worker:restart`:

```bash
vendor/bin/message-bus-worker worker:pause \
    --bootstrap=config/message-bus-worker.php \
    --worker-instance-id=worker.importer.host.42.abcd \
    --created-by=deploy \
    --idempotency-key=deploy-2026-11-03-pause
```

A control command requires an explicit target: worker ID/name/instance/group,
transport, queue, flow, binding ID/pattern, mode, host, or `--all`. Audit
options preserve source, actor, reason, request/correlation identifiers,
expiry, idempotency, and override semantics. Success prints the command ID,
type, and duplicate flag.

## Inspect workers

```bash
vendor/bin/message-bus-worker worker:status \
    --bootstrap=config/message-bus-worker.php \
    --group=imports \
    --children
```

With no target filters, status defaults to all registered workers. `--children`
adds child process/job rows when the configured status service exposes them.

## Generate the control schema

Print the PostgreSQL DDL:

```bash
vendor/bin/message-bus-worker worker:schema:postgres
```

Or write it to an application migration artifact:

```bash
vendor/bin/message-bus-worker worker:schema:postgres \
    --output=resources/sql/message-bus-worker.sql
```

The command generates only worker-control tables. The core queue schema and
application migration bookkeeping remain owned by their respective packages.

## Process boundary

The executable runs one cooperative AMPHP worker. It intentionally has no
`auto/fork` mode and does not manage a process pool. Multi-process scaling,
forced termination, and worker replacement belong to a separate supervisor or
future optional process package; the async worker kernel stays usable without
those dependencies.
