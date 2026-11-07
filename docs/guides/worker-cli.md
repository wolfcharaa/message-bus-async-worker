# Worker CLI

Пакет устанавливает `vendor/bin/message-bus-worker` — единый вход для queue
workers и worker-control operations. CLI использует Symfony Console, но не ищет
и не импортирует контейнер приложения. Host application передаёт явный PHP
bootstrap, возвращающий `WorkerRuntime` или
`WorkerRuntimeProviderInterface`.

## Bootstrap

Если lifecycle процесса управляется вручную, создайте, например,
`config/message-bus-worker.php`:

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

`createQueueWorkerRunner()` и `createWorkerControlRuntime()` обозначают
composition functions приложения. Runner обязателен. Control опционален для
простого queue execution; без него команды `worker:control`, action aliases и
status завершаются понятной ошибкой. Runner и CLI должны использовать один
`WorkerControlRuntime`.

Если контейнер владеет pools, clients, scopes или другими disposable resources,
верните provider. Это рекомендуемый вариант:

```php
<?php

declare(strict_types=1);

use Fedor\MessageBus\Worker\Runtime\WorkerRuntime;
use Fedor\MessageBus\Worker\Runtime\WorkerRuntimeProviderInterface;
use Thesis\Dic;

return new class implements WorkerRuntimeProviderInterface {
    public function run(\Closure $operation): mixed
    {
        return Dic::run(
            new ApplicationWorkerRuntimeModule(),
            static fn (WorkerRuntime $runtime): mixed => $operation($runtime),
        );
    }
};
```

`ApplicationWorkerRuntimeModule` предоставляет приложение. Thesis DIC здесь
лишь пример lifecycle и не является обязательной зависимостью worker package.
Callback provider должен сохранять runtime валидным всю operation и освобождать
ресурсы и при успехе, и при исключении. Все команды, включая status/control,
обращаются к runtime только внутри callback.

Bootstrap ищется в таком порядке:

1. `--bootstrap`;
2. `MESSAGE_BUS_WORKER_BOOTSTRAP`;
3. `message-bus-worker.php`, `config/message-bus-worker.php`, затем
   `config/message_bus_worker.php` в текущем каталоге.

Результат должен быть `WorkerRuntimeProviderInterface` или прямым
`WorkerRuntime`. Прямой runtime сохранён для совместимости, когда teardown
выполняет host. `BootstrapResolver::run()` принимает оба варианта;
`resolve()` намеренно принимает только прямой runtime, чтобы managed runtime
не покинул свой lifecycle. Интеграция PSR-11/framework container выполняется в
application bootstrap.

## Запустить Worker

```shell
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

Команда переносит значения CLI в `ConsumerOptions` и
`QueueWorkerRunnerOptions`. Доступны timeout/limit/lock TTL, filters,
ограничения messages/runtime/idle, idle delay, memory limit в bytes, интервалы
heartbeat/cancellation/control, graceful shutdown timeout и stop-on-empty.
Актуальные defaults показывает:

```shell
vendor/bin/message-bus-worker worker:run --help
```

При выходе печатается machine-readable summary:

```text
handled=10 succeeded=8 retried=1 rejected=0 cancelled=1 lease_lost=0
```

Обычное завершение возвращает `0`. Durable restart возвращает
`--restart-exit-code`, default `75`, чтобы supervisor заменил процесс.
Ошибки bootstrap, configuration, storage или handler дают non-zero exit code.

При наличии `ext-pcntl` сигналы `SIGTERM`, `SIGINT` и `SIGQUIT`
запрашивают cooperative shutdown: новые jobs не claims, текущая получает grace
period, durable transition завершается до выхода. Старые signal handlers
восстанавливаются. CPU-bound код без suspension таким способом не прерывается.

## Управлять Workers

Общая команда:

```shell
vendor/bin/message-bus-worker worker:control \
  --bootstrap=config/message-bus-worker.php \
  --command=drain \
  --group=imports \
  --reason='deployment'
```

Action aliases: `worker:pause`, `worker:resume`, `worker:drain`,
`worker:stop`, `worker:kill` и `worker:restart`.

```shell
vendor/bin/message-bus-worker worker:pause \
  --bootstrap=config/message-bus-worker.php \
  --worker-instance-id=worker.importer.host.42.abcd \
  --created-by=deploy \
  --idempotency-key=deploy-2026-11-03-pause
```

Control command требует явную цель: worker ID/name/instance/group, transport,
queue, flow, binding ID/pattern, mode, host или `--all`. Audit options
сохраняют source, actor, reason, request/correlation IDs, expiry, idempotency и
override. Успех печатает command ID, type и duplicate flag.

## Посмотреть Состояние

```shell
vendor/bin/message-bus-worker worker:status \
  --bootstrap=config/message-bus-worker.php \
  --group=imports \
  --children
```

Без target filters status показывает все зарегистрированные workers.
`--children` добавляет child process/job rows, если status service их
поддерживает.

## Сгенерировать Schema

Вывести PostgreSQL DDL:

```shell
vendor/bin/message-bus-worker worker:schema:postgres
```

Записать его в application migration artifact:

```shell
vendor/bin/message-bus-worker worker:schema:postgres \
  --output=resources/sql/message-bus-worker.sql
```

Команда генерирует только worker-control tables. Core queue schema и migration
bookkeeping остаются ответственностью соответствующих пакетов и приложения.

## Граница Процесса

Executable запускает один cooperative AMPHP worker. В нём намеренно нет
`auto/fork` и process pool. Multi-process scaling, hard termination и
replacement выполняет внешний supervisor или отдельный process package.
