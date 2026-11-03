# Worker Control Reference

The worker control API preserves the proven v6 model while using AMPHP
cancellation and `amphp/postgres`. It is split into neutral contracts under
`Fedor\MessageBus\Worker\Control` and the PostgreSQL adapter under
`Fedor\MessageBus\Worker\Control\Postgres`.

## Command service

`DefaultWorkerControlService` exposes `pause`, `resume`, `drain`, `stop`,
`kill`, `restart`, and direct `send`. `WorkerControlRequest` carries the audit
source, actor, reason, request/correlation identifiers, expiry, idempotency key,
and override flag.

Pause and resume write both a command and a durable desired state. Replaying an
idempotency key returns the original receipt with `duplicate = true` and does
not write the desired state again. Drain, stop, kill, and restart are one-shot
commands and must expire. The service supplies the v6 defaults when the request
does not specify an expiry:

- kill: 60 seconds;
- drain, stop, and restart: 300 seconds.

## Targeting and resolution

`WorkerTarget` supports global, worker ID/name/instance/group, transport, queue,
flow, binding ID/pattern, mode, and host selectors. A global target cannot be
combined with another selector. Binding patterns use `*` as the wildcard.

Desired states are resolved in the same order as v6: highest target
specificity first, then newest creation time. Only active states participate.
Listing states uses overlapping target selectors rather than exact target
equality.

## PostgreSQL gateway

`PostgresWorkerControlGateway` implements all current control storage ports:

- `WorkerControlCommandRepositoryInterface`;
- `WorkerDesiredStateRepositoryInterface`;
- `WorkerControlInboxInterface`;
- `WorkerRegistryInterface`;
- `WorkerStatusRepositoryInterface`.

Construct one gateway and share it between the service, worker inbox, registry,
and operational status readers:

```php
use Amp\Postgres\PostgresConnectionPool;
use Fedor\MessageBus\Worker\Control\DefaultWorkerControlService;
use Fedor\MessageBus\Worker\Control\Postgres\PostgresWorkerControlGateway;

/** @var PostgresConnectionPool $postgres */
$control = new PostgresWorkerControlGateway($postgres);
$service = new DefaultWorkerControlService($control, $control);
```

Use `PostgresWorkerControlSchemaGenerator` from a migration or installer. A
custom `WorkerControlTableDefinition` may change table names, but each name must
be a plain PostgreSQL identifier without a schema prefix.

The generator creates the complete v6-shaped table set for commands, desired
state, workers, children, deliveries, acknowledgements, and audit. Delivery and
audit rows are reserved for later process adapters; the current gateway does
not write them. Schema version bookkeeping is intentionally left to the host
application migration system.

## Inbox cursor

`receive(identity, cursor, cancellation)` scans non-expired commands in
database insertion order. It returns only commands matching the worker, but its
next cursor points to the last scanned command, including a command for another
worker. This prevents an unrelated command from being rescanned forever.

The cursor stores the public command ID. If that row no longer exists, the
gateway follows v6 behavior and starts from the beginning of the remaining
non-expired command set. Callers that delete command history must therefore
accept replay or keep a retention window longer than the worker outage window.

## Worker registry and acknowledgements

Register a `WorkerInstance` before registering children or acknowledging a
command. PostgreSQL foreign keys intentionally enforce this lifecycle.
Heartbeat updates state, activity, child count, and the last applied command;
stop records final state, timestamp, and optional failure. Child registration,
heartbeat, and finish operations preserve queue/message correlation fields for
operational inspection.

Acknowledgement writes are idempotent per command and worker instance. A later
write replaces state, time, message, and error details. Status readers can list
workers by `WorkerTarget`, list children for a worker, and list acknowledgements
for a command.

## Runner integration

Pass an optional `WorkerControlRuntime` to `QueueWorkerRunner`. The runner then
registers a single-mode worker identity before its first claim, polls the inbox
between jobs and from the concurrent job monitor, heartbeats idle/busy state,
acknowledges applied commands, and records its final lifecycle.

Runtime behavior preserves the v6 state machine:

| Command/state | Runner behavior |
| --- | --- |
| pause / desired paused | Stops new claims and keeps polling for resume or a terminal command |
| resume / desired resumed | Returns to claims unless a terminal command was already observed |
| drain | Lets the current job finish, then exits |
| stop | Lets the current job finish, then exits |
| kill | Cancels the cooperative current handler; retry/reject still follows its persisted attempt budget |
| restart | Lets the current job finish, records `restarting`, and returns the configured restart exit code |

`QueueWorkerRunnerOptions` supplies worker name/group/instance/host selectors,
control polling interval, and restart exit code. Supplying no
`WorkerControlRuntime` preserves the queue-only runner behavior and performs no
control-plane I/O.

## Cancellation and current runtime limit

Every storage method accepts `Amp\Cancellation` and checks it before AMPHP I/O
and while consuming multi-row results. AMPHP PostgreSQL 2.x does not cancel an
already-running statement, so an in-flight query is allowed to reach a definite
database result before cancellation is observed at the next safe boundary.

Control kill, process shutdown, durable queue cancellation, and lease loss are
kept as distinct causes. Kill follows the v6 terminated-job retry/reject
semantics; durable queue cancellation persists `cancelled`; lease loss prevents
the stale worker from writing a transition.

The current runner is cooperative and single-process. It cannot interrupt
CPU-bound code that never suspends, and it does not send operating-system
signals. The bundled executable optionally translates process stop signals into
AMPHP cancellation, but it deliberately does not implement the v6 `auto/fork`
mode. A multi-process supervisor remains a separate package boundary.
