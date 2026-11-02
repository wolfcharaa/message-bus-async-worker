<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue\Postgres;

use Amp\Cancellation;
use Amp\Postgres\PostgresExecutor;
use Amp\Postgres\PostgresLink;
use DateTimeImmutable;
use DateTimeZone;
use Fedor\MessageBus\Queue\QueueJobState;
use Fedor\MessageBus\Queue\QueueTableDefinition;
use Fedor\MessageBus\Worker\Clock\UtcSystemClock;
use Fedor\MessageBus\Worker\Queue\ConsumerOptions;
use Fedor\MessageBus\Worker\Queue\MessageConsumerInterface;
use Fedor\MessageBus\Worker\Queue\QueueLeaseControlInterface;
use Fedor\MessageBus\Worker\Queue\QueueLeaseLost;
use Fedor\MessageBus\Worker\Queue\ReceivedQueueMessage;
use JsonException;
use Psr\Clock\ClockInterface;
use RuntimeException;

final class PostgresMessageConsumer implements MessageConsumerInterface, QueueLeaseControlInterface
{
    private readonly string $table;
    private readonly PostgresQueueRecordMapper $records;
    private readonly RetryDelayCalculator $retryDelays;

    public function __construct(
        private readonly PostgresLink $postgres,
        private readonly ClockInterface $clock = new UtcSystemClock(),
        QueueTableDefinition $definition = new QueueTableDefinition(),
        ?PostgresQueueRecordMapper $records = null,
        ?RetryDelayCalculator $retryDelays = null,
    ) {
        $this->table = $this->quoteIdentifier($definition->tableName);
        $this->records = $records ?? new PostgresQueueRecordMapper();
        $this->retryDelays = $retryDelays ?? new RetryDelayCalculator();
    }

    #[\Override]
    public function next(ConsumerOptions $options, Cancellation $cancellation): ?ReceivedQueueMessage
    {
        $cancellation->throwIfRequested();
        $this->recoverStale($options, $cancellation);
        $transaction = $this->postgres->beginTransaction();

        try {
            $cancellation->throwIfRequested();
            $now = $this->clock->now();
            [$where, $params] = $this->claimFilter($options, $now);
            $result = $transaction->execute(
                'SELECT id FROM ' . $this->table . '
                 WHERE ' . implode(' AND ', $where) . '
                 ORDER BY priority DESC, available_at ASC, id ASC
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1',
                $params,
            );
            $row = $result->fetchRow();
            if ($row === null) {
                $transaction->commit();

                return null;
            }

            $cancellation->throwIfRequested();
            $claimed = $transaction->execute(
                'UPDATE ' . $this->table . '
                 SET status = :running,
                     attempts = attempts + 1,
                     locked_at = :now,
                     locked_by = :worker,
                     heartbeat_at = :now,
                     started_at = :now,
                     updated_at = :now
                 WHERE id = :id AND status = :pending
                 RETURNING *',
                [
                    'running' => QueueJobState::Running->value,
                    'now' => $this->formatDate($now),
                    'worker' => $options->workerId,
                    'id' => $this->records->identifier($row, 'id'),
                    'pending' => QueueJobState::Pending->value,
                ],
            )->fetchRow();
            if ($claimed === null) {
                throw new RuntimeException('PostgreSQL queue claim did not return the leased job.');
            }

            $transaction->commit();

            return $this->records->received($claimed);
        } catch (\Throwable $error) {
            if ($transaction->isActive()) {
                $transaction->rollback();
            }

            throw $error;
        }
    }

    #[\Override]
    public function ack(ReceivedQueueMessage $message, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        $now = $this->clock->now();
        $this->leaseUpdate(
            $this->postgres,
            $message,
            'status = :succeeded,
             finished_at = :now,
             locked_at = NULL,
             locked_by = NULL,
             heartbeat_at = NULL,
             updated_at = :now',
            ['succeeded' => QueueJobState::Succeeded->value, 'now' => $this->formatDate($now)],
        );
    }

    #[\Override]
    public function retry(
        ReceivedQueueMessage $message,
        \Throwable $reason,
        Cancellation $cancellation,
    ): void {
        $cancellation->throwIfRequested();
        $this->retryOn($this->postgres, $message, $reason, $this->clock->now());
    }

    #[\Override]
    public function reject(
        ReceivedQueueMessage $message,
        \Throwable $reason,
        Cancellation $cancellation,
    ): void {
        $cancellation->throwIfRequested();
        $this->rejectOn($this->postgres, $message, $reason, $this->clock->now());
    }

    #[\Override]
    public function cancel(
        ReceivedQueueMessage $message,
        \Throwable $reason,
        Cancellation $cancellation,
    ): void {
        $cancellation->throwIfRequested();
        $now = $this->clock->now();
        $error = $this->error($reason);
        $this->leaseUpdate(
            $this->postgres,
            $message,
            'status = :cancelled,
             finished_at = :now,
             locked_at = NULL,
             locked_by = NULL,
             heartbeat_at = NULL,
             last_error = :last_error,
             last_error_details = :last_error_details,
             updated_at = :now',
            [
                'cancelled' => QueueJobState::Cancelled->value,
                'now' => $this->formatDate($now),
                'last_error' => $error['summary'],
                'last_error_details' => $this->jsonEncode($error['details']),
            ],
        );
    }

    #[\Override]
    public function heartbeat(ReceivedQueueMessage $message, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        $this->leaseUpdate(
            $this->postgres,
            $message,
            'heartbeat_at = :now, updated_at = :now',
            ['now' => $this->formatDate($this->clock->now())],
        );
    }

    #[\Override]
    public function isCancellationRequested(
        ReceivedQueueMessage $message,
        Cancellation $cancellation,
    ): bool {
        $cancellation->throwIfRequested();
        $row = $this->postgres->execute(
            'SELECT cancellation_requested FROM ' . $this->table . '
             WHERE id = :id AND status = :running AND locked_by = :lease_owner',
            [
                'id' => $message->queueMessageId,
                'running' => QueueJobState::Running->value,
                'lease_owner' => $message->leaseOwner,
            ],
        )->fetchRow();
        if ($row === null) {
            throw QueueLeaseLost::forMessage($message);
        }

        return $this->records->boolean($row, 'cancellation_requested');
    }

    #[\Override]
    public function recoverStale(ConsumerOptions $options, Cancellation $cancellation): int
    {
        $cancellation->throwIfRequested();
        $transaction = $this->postgres->beginTransaction();

        try {
            $now = $this->clock->now();
            $threshold = $now->modify('-' . $options->lockTtlSeconds . ' seconds');
            $result = $transaction->execute(
                'SELECT * FROM ' . $this->table . '
                 WHERE status = :running
                   AND transport = :transport
                   AND queue = :queue
                   AND COALESCE(heartbeat_at, locked_at) < :threshold
                 ORDER BY updated_at ASC, id ASC
                 FOR UPDATE SKIP LOCKED',
                [
                    'running' => QueueJobState::Running->value,
                    'transport' => $options->transport,
                    'queue' => $options->queue,
                    'threshold' => $this->formatDate($threshold),
                ],
            );
            $rows = iterator_to_array($result, false);
            foreach ($rows as $row) {
                $cancellation->throwIfRequested();
                $this->retryOn(
                    $transaction,
                    $this->records->received($row),
                    new RuntimeException(sprintf(
                        'Worker heartbeat expired after %d seconds.',
                        $options->lockTtlSeconds,
                    )),
                    $now,
                );
            }
            $transaction->commit();

            return count($rows);
        } catch (\Throwable $error) {
            if ($transaction->isActive()) {
                $transaction->rollback();
            }

            throw $error;
        }
    }

    private function retryOn(
        PostgresExecutor $executor,
        ReceivedQueueMessage $message,
        \Throwable $reason,
        DateTimeImmutable $now,
    ): void {
        if (!$message->canRetry()) {
            $this->rejectOn($executor, $message, $reason, $now);

            return;
        }

        $error = $this->error($reason);
        $availableAt = $now->modify(sprintf(
            '+%d seconds',
            $this->retryDelays->seconds($message->message->retryPolicySnapshot, $message->attempts),
        ));
        $this->leaseUpdate(
            $executor,
            $message,
            'status = :pending,
             available_at = :available_at,
             locked_at = NULL,
             locked_by = NULL,
             heartbeat_at = NULL,
             last_error = :last_error,
             last_error_details = :last_error_details,
             updated_at = :now',
            [
                'pending' => QueueJobState::Pending->value,
                'available_at' => $this->formatDate($availableAt),
                'last_error' => $error['summary'],
                'last_error_details' => $this->jsonEncode($error['details']),
                'now' => $this->formatDate($now),
            ],
        );
    }

    private function rejectOn(
        PostgresExecutor $executor,
        ReceivedQueueMessage $message,
        \Throwable $reason,
        DateTimeImmutable $now,
    ): void {
        $error = $this->error($reason);
        $this->leaseUpdate(
            $executor,
            $message,
            'status = :failed,
             finished_at = :now,
             locked_at = NULL,
             locked_by = NULL,
             heartbeat_at = NULL,
             last_error = :last_error,
             last_error_details = :last_error_details,
             updated_at = :now',
            [
                'failed' => QueueJobState::Failed->value,
                'now' => $this->formatDate($now),
                'last_error' => $error['summary'],
                'last_error_details' => $this->jsonEncode($error['details']),
            ],
        );
    }

    /**
     * @param array<string, bool|float|int|string|null> $fields
     */
    private function leaseUpdate(
        PostgresExecutor $executor,
        ReceivedQueueMessage $message,
        string $assignments,
        array $fields,
    ): void {
        $result = $executor->execute(
            'UPDATE ' . $this->table . '
             SET ' . $assignments . '
             WHERE id = :id AND status = :running AND locked_by = :lease_owner',
            $fields + [
                'id' => $message->queueMessageId,
                'running' => QueueJobState::Running->value,
                'lease_owner' => $message->leaseOwner,
            ],
        );
        if ($result->getRowCount() !== 1) {
            throw QueueLeaseLost::forMessage($message);
        }
    }

    /**
     * @return array{0: list<string>, 1: array<string, bool|float|int|string|null>}
     */
    private function claimFilter(ConsumerOptions $options, DateTimeImmutable $now): array
    {
        $where = [
            'status = :pending',
            'transport = :transport',
            'queue = :queue',
            'available_at <= :now',
        ];
        $params = [
            'pending' => QueueJobState::Pending->value,
            'transport' => $options->transport,
            'queue' => $options->queue,
            'now' => $this->formatDate($now),
        ];

        if ($options->flows !== []) {
            $where[] = 'flow IN (' . $this->placeholders('flow', $options->flows, $params) . ')';
        }
        if ($options->bindingIds !== []) {
            $where[] = 'binding_id IN (' . $this->placeholders('binding', $options->bindingIds, $params) . ')';
        }
        foreach ($options->bindingPatterns as $index => $pattern) {
            $parameter = 'binding_pattern_' . $index;
            $where[] = 'binding_id LIKE :' . $parameter;
            $params[$parameter] = str_replace('*', '%', $pattern);
        }

        return [$where, $params];
    }

    /**
     * @param list<string> $values
     * @param array<string, bool|float|int|string|null> $params
     */
    private function placeholders(string $prefix, array $values, array &$params): string
    {
        $placeholders = [];
        foreach ($values as $index => $value) {
            $parameter = $prefix . '_' . $index;
            $placeholders[] = ':' . $parameter;
            $params[$parameter] = $value;
        }

        return implode(', ', $placeholders);
    }

    /** @return array{summary: string, details: array<string, mixed>} */
    private function error(\Throwable $reason): array
    {
        return [
            'summary' => $reason->getMessage(),
            'details' => [
                'class' => $reason::class,
                'message' => $reason->getMessage(),
                'code' => $reason->getCode(),
                'file' => $reason->getFile(),
                'line' => $reason->getLine(),
                'trace' => substr($reason->getTraceAsString(), 0, 8000),
            ],
        ];
    }

    /** @param array<string, mixed> $value */
    private function jsonEncode(array $value): string
    {
        try {
            return json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        } catch (JsonException $error) {
            throw new RuntimeException('PostgreSQL queue value cannot be encoded as JSON.', previous: $error);
        }
    }

    private function formatDate(DateTimeImmutable $value): string
    {
        return $value->setTimezone(new DateTimeZone('UTC'))->format(DATE_ATOM);
    }

    private function quoteIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }
}
