# Worker Control

Control API сохраняет модель v6, используя AMPHP cancellation и
`amphp/postgres`. Нейтральные contracts находятся в
`Fedor\MessageBus\Worker\Control`, PostgreSQL adapter — в
`Fedor\MessageBus\Worker\Control\Postgres`.

## Command Service

`DefaultWorkerControlService` предоставляет `pause`, `resume`, `drain`,
`stop`, `kill`, `restart` и прямой `send`. `WorkerControlRequest`
содержит audit source, actor, reason, request/correlation IDs, expiry,
idempotency key и override.

Pause/resume записывают command и durable desired state. Повтор idempotency key
возвращает исходный receipt с `duplicate = true` и не записывает desired state
заново. Drain, stop, kill и restart являются one-shot и должны иметь expiry.
Defaults:

- kill — 60 секунд;
- drain, stop и restart — 300 секунд.

## Targeting И Resolution

`WorkerTarget` поддерживает global, worker ID/name/instance/group, transport,
queue, flow, binding ID/pattern, mode и host. Global target нельзя сочетать с
другим selector. В binding pattern `*` является wildcard.

Desired states разрешаются сначала по наибольшей specificity target, затем по
новейшему времени создания. Участвуют только active states. Listing использует
пересекающиеся selectors, а не exact equality.

## PostgreSQL Gateway

`PostgresWorkerControlGateway` реализует:

- `WorkerControlCommandRepositoryInterface`;
- `WorkerDesiredStateRepositoryInterface`;
- `WorkerControlInboxInterface`;
- `WorkerRegistryInterface`;
- `WorkerStatusRepositoryInterface`.

Один gateway должен использоваться service, inbox, registry и status readers:

```php
use Amp\Postgres\PostgresConnectionPool;
use Fedor\MessageBus\Worker\Control\DefaultWorkerControlService;
use Fedor\MessageBus\Worker\Control\Postgres\PostgresWorkerControlGateway;

/** @var PostgresConnectionPool $postgres */
$control = new PostgresWorkerControlGateway($postgres);
$service = new DefaultWorkerControlService($control, $control);
```

`PostgresWorkerControlSchemaGenerator` вызывается из migration или installer.
`WorkerControlTableDefinition` может менять имена таблиц; имя должно быть
простым PostgreSQL identifier без schema prefix.

Generator создаёт tables commands, desired state, workers, children,
deliveries, acknowledgements и audit. Delivery/audit зарезервированы для
будущих process adapters и пока не записываются. Schema version bookkeeping
остаётся у migration system приложения.

## Inbox Cursor

`receive(identity, cursor, cancellation)` читает non-expired commands в
порядке вставки. Он возвращает только подходящие worker команды, но следующий
cursor указывает на последнюю просмотренную команду, включая чужую. Поэтому
нерелевантная команда не сканируется бесконечно.

Cursor хранит public command ID. Если строка удалена, gateway начинает с начала
оставшегося non-expired набора. При удалении history нужно принимать replay или
держать retention дольше максимального outage worker.

## Registry И Acknowledgements

`WorkerInstance` регистрируется до children и acknowledgements — это
обеспечивают PostgreSQL foreign keys. Heartbeat обновляет state, activity,
children count и last applied command; stop записывает финальные state/time и
optional failure. Child lifecycle сохраняет queue/message correlation.

Acknowledgement идемпотентен по command и worker instance. Новая запись заменяет
state, time, message и error. Status readers фильтруют workers через
`WorkerTarget`, показывают children и acknowledgements.

## Интеграция С Runner

Передайте optional `WorkerControlRuntime` в `QueueWorkerRunner`. Runner
регистрирует identity до первого claim, опрашивает inbox между jobs и из job
monitor, пишет heartbeat и финальный lifecycle.

| Command/state | Поведение runner |
| --- | --- |
| pause / desired paused | не claims новые jobs, ждёт resume/terminal command |
| resume / desired resumed | возвращается к claims, если terminal command не получен |
| drain | завершает текущую job и выходит |
| stop | завершает текущую job и выходит |
| kill | cooperative-отменяет handler; retry/reject использует attempt budget |
| restart | завершает job, пишет `restarting`, возвращает restart exit code |

Без `WorkerControlRuntime` runner работает только с queue и не выполняет
control-plane I/O.

## Cancellation И Ограничения

Storage methods принимают `Amp\Cancellation` и проверяют его до I/O и при
чтении нескольких rows. Уже выполняющийся query не отменяется AMPHP PostgreSQL
2.x и достигает определённого database result.

Control kill, process shutdown, durable queue cancellation и lease loss —
разные причины. Kill применяет retry/reject, durable cancellation сохраняет
`cancelled`, lease loss запрещает stale worker записать transition.

Текущий runner cooperative и single-process. Он не прерывает CPU-bound код без
suspension и не посылает OS signals. Bundled executable может переводить stop
signals в cancellation, но не реализует v6 `auto/fork`. Multi-process
supervisor остаётся отдельной границей.
