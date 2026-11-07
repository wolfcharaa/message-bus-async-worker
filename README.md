# Message Bus Async Worker

Опциональный AMPHP runtime и Symfony Console CLI для
[`romanfedorskij/message-bus-async`](https://github.com/wolfcharaa/message-bus-async).
Пакет получает durable jobs из PostgreSQL, открывает application scope на job,
выполняет точный queued binding и сохраняет результат с проверкой lease.

Линия `0.1.x` требует совместимый core `^0.1`.

## Когда Пакет Нужен

- durable messages из core нужно выполнять отдельными процессами;
- job должен иметь heartbeat, retry, cancellation и stale recovery;
- обработчики должны получать новый service scope для каждой job;
- workers нужно ставить на pause, resume, drain, stop или restart;
- runtime должен работать кооперативно в AMPHP event loop без скрытого fork pool.

## Возможности

| Задача | Тип или команда | Документ |
| --- | --- | --- |
| Claim и guarded transitions PostgreSQL job | `PostgresMessageConsumer` | [Consumer](docs/reference/postgres-consumer.md) |
| Кооперативный consumer loop | `QueueWorkerRunner` | [Runner](docs/reference/worker-runner.md) |
| Выполнение точного queued binding | `MessageBusQueueWorker` | [Runner](docs/reference/worker-runner.md) |
| Scope на одну job | `WorkerModule` или application scope factory | [CLI](docs/guides/worker-cli.md) |
| Durable control и status | `PostgresWorkerControlGateway` | [Control plane](docs/reference/worker-control.md) |
| Операционный вход | `vendor/bin/message-bus-worker` | [CLI](docs/guides/worker-cli.md) |

Core отвечает за dispatch, envelope codecs, enqueue/status/cancel и общую queue
schema. Зависимость направлена только от worker к core.

## Требования

- PHP `8.4.12` или новее в пределах PHP 8;
- `romanfedorskij/message-bus-async ^0.1`;
- AMPHP 3 и `amphp/postgres` 2;
- `ext-pgsql` или `ext-pq`;
- Symfony Console 7.4 или 8.1.

`ext-pcntl` и Thesis DIC опциональны. Без PCNTL shutdown запрашивается через
worker control или другой host adapter.

## Установка

До регистрации пакетов в Packagist добавьте оба GitHub VCS repository:

```shell
composer config repositories.message-bus-async vcs \
  https://github.com/wolfcharaa/message-bus-async.git
composer config repositories.message-bus-async-worker vcs \
  https://github.com/wolfcharaa/message-bus-async-worker.git
composer require romanfedorskij/message-bus-async-worker:^0.1
```

Executable появится как `vendor/bin/message-bus-worker`.

## Первый Worker

Host application предоставляет PHP bootstrap, который возвращает
`WorkerRuntime` или рекомендуемый `WorkerRuntimeProviderInterface`. Provider
удерживает pools, clients и container scopes на протяжении всей CLI operation и
закрывает их после неё.

```shell
vendor/bin/message-bus-worker worker:run \
  --bootstrap=config/message-bus-worker.php \
  --transport=postgres \
  --queue=addresses
```

Пакет не ищет application container и не создаёт schema при старте. Сгенерируйте
worker-control DDL командой `worker:schema:postgres` и примените его через
versioned migration приложения. Полные варианты bootstrap и команды управления
приведены в [руководстве по CLI](docs/guides/worker-cli.md).

## Как Выполняется Job

1. Consumer claims одну доступную строку и фиксирует lease.
2. Runner открывает новый job scope и разрешает queue worker.
3. Параллельный monitor обновляет heartbeat и проверяет cancellation/control.
4. Точный binding из producer выполняется локально и не ставится в очередь
   второй раз.
5. Consumer делает ack, retry, reject или cancel с проверкой lease owner, затем
   scope закрывается.

Delivery — at-least-once. После crash или stale lease сообщение может
выполниться повторно, поэтому state-changing handlers должны быть
идемпотентными. CPU-bound handler обязан иметь cooperative suspension points
или выполняться через отдельный process adapter.

## Граница Процесса

Один CLI process запускает один cooperative worker. Количество процессов,
restart по exit code `75`, hard kill и resource limits настраиваются в systemd,
Supervisor, Kubernetes, Docker или другом внешнем supervisor. Пакет не создаёт
скрытый fork pool.

## Документация

1. [Worker CLI](docs/guides/worker-cli.md) — bootstrap, run, status, control,
   schema и exit codes.
2. [PostgreSQL consumer](docs/reference/postgres-consumer.md) — claim, lease,
   transitions, retry и stale recovery.
3. [Worker runner](docs/reference/worker-runner.md) — scopes, heartbeat,
   cancellation и outcome mapping.
4. [Worker control](docs/reference/worker-control.md) — desired state, targeting,
   acknowledgements и storage ownership.
5. [Core queue contract](https://github.com/wolfcharaa/message-bus-async/blob/v0.1.0/docs/reference/postgres-queue.md)
   — producer-side schema и delivery guarantees.

## Разработка

Source workspace ожидает core checkout в `../MessageBusAsync`; Composer
подключает его как `0.1.0` через path repository.

```shell
composer validate --strict
composer analyse
composer test
```

Optional integration suite требует disposable PostgreSQL database:

```shell
MESSAGE_BUS_TEST_POSTGRES_CONFIG='host=127.0.0.1 port=5432 user=test password=test db=test' \
  composer test:integration
```

## Лицензия

[MIT](LICENSE)
