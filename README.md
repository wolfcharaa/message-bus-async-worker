# Message Bus Async Worker

Optional operational worker runtime for
`romanfedorskij/message-bus-async`.

> Status: pre-release `0.1.0` scaffold. Worker execution is not implemented yet
> and the package is not ready for application use.

## Planned Responsibility

The worker package will own:

- durable message consumption and ack/retry/reject execution;
- per-job lifecycle scopes and cancellation;
- heartbeat, stale-job recovery, and worker control plane;
- a standalone Symfony Console worker executable;
- optional process and signal adapters.

Producer, status, cancellation, common queue schema, message registry, and local
dispatch remain in the core package. The dependency direction is always worker
to core; the core package must not reference worker symbols.

## Platform

- PHP `8.4.12` or newer within the PHP 8 major line;
- `romanfedorskij/message-bus-async` `0.1.x`.

Console, AMPHP, PostgreSQL, and process dependencies will be declared only with
the implementation slice that uses them.

## Development State

The repository currently contains package metadata only. Installation commands,
runtime examples, guarantees, and contributor checks will be documented after
the corresponding implementation and executable tests exist.
