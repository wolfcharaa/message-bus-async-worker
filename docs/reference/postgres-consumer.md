# PostgreSQL Consumer

`PostgresMessageConsumer` — worker-side реализация queue schema из
`romanfedorskij/message-bus-async`. Он реализует
`MessageConsumerInterface` и `QueueLeaseControlInterface` через
`amphp/postgres`; blocking PDO path отсутствует.

## Claim Contract

`next(ConsumerOptions, Cancellation)` сначала восстанавливает stale jobs для
выбранных transport/queue, затем claims одну доступную `pending` строку:

1. фильтрует transport, queue, availability и optional flow/binding;
2. сортирует по убыванию priority, возрастанию availability и queue ID;
3. блокирует одну строку через `FOR UPDATE SKIP LOCKED`;
4. переводит её в `running`, увеличивает attempts и записывает worker ID,
   lock/heartbeat/start/update timestamps;
5. делает commit до возврата `ReceivedQueueMessage`.

Результат содержит queue ID, декодированный versioned
`SerializedEnvelope`, immutable retry snapshot, attempt и lease owner. Сырая
PostgreSQL row наружу не передаётся.

Defaults `ConsumerOptions`: timeout 5 секунд, limit 1, worker ID
`message-bus-worker`, lock TTL 300 секунд. Текущий PostgreSQL claim возвращает
одно сообщение; timeout и multi-message limit относятся к runner.

## Lease Invariant

Ack, retry, reject, cancel и heartbeat обновляют строку только при совпадении:

- queue ID;
- `status = running`;
- `locked_by` с lease owner полученного сообщения.

Ноль изменённых строк приводит к `QueueLeaseLost`. Медленный или
возобновившийся worker не может записать результат после передачи stale job
другому worker.

`isCancellationRequested()` использует тот же guard. Producer
`requestCancellation()` только сохраняет флаг; worker должен его опрашивать и
сохранить `cancelled` после cooperative stop.

## Durable Transitions

| Операция | Из | В | Эффект |
| --- | --- | --- | --- |
| `ack` | `running` | `succeeded` | finish time, lease очищен |
| `retry` | `running` | `pending` | next availability/error, lease очищен |
| exhausted `retry` | `running` | `failed` | finish/error, lease очищен |
| `reject` | `running` | `failed` | finish/error, lease очищен |
| `cancel` | `running` | `cancelled` | finish/error, lease очищен |
| `heartbeat` | `running` | `running` | timestamps обновлены |

Fixed и exponential delays используют сохранённый attempt. Error details
содержат class, message, code, source location и ограниченный stack trace.
Handlers обязаны использовать безопасные exception messages без secrets.

## Stale Recovery

`recoverStale()` выбирает `running` rows, где последний heartbeat/lock старше
TTL. `FOR UPDATE SKIP LOCKED` защищает выборку, все строки меняются в одной
transaction. Recoverable jobs возвращаются в `pending` со своим retry delay,
исчерпанные — в `failed`.

Cancellation между строками откатывает всю recovery transaction. Уже
выполняющийся SQL в AMPHP PostgreSQL 2.x не отменяется: consumer ждёт
определённого результата и проверяет cancellation на следующей безопасной
границе.

## Интеграция С Runner

`QueueWorkerRunner` владеет consumer loop, scope на job,
heartbeat/cancellation monitor, выбором durable outcome и graceful process
cancellation. Job-scoped `QueueWorkerInterface` десериализует envelope и
выполняет точный queued binding. Подробности — в
[справочнике runner](worker-runner.md).
