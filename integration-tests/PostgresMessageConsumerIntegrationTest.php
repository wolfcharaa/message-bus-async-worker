<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\IntegrationTests;

use Amp\NullCancellation;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use DateInterval;
use DateTimeImmutable;
use Fedor\MessageBus\Envelope\SerializedEnvelope;
use Fedor\MessageBus\Queue\Postgres\PostgresQueueGateway;
use Fedor\MessageBus\Queue\Postgres\PostgresQueueSchemaGenerator;
use Fedor\MessageBus\Queue\QueueJobState;
use Fedor\MessageBus\Queue\QueueMessage;
use Fedor\MessageBus\Queue\QueueTableDefinition;
use Fedor\MessageBus\Queue\RetryPolicySnapshot;
use Fedor\MessageBus\Worker\Queue\ConsumerOptions;
use Fedor\MessageBus\Worker\Queue\Postgres\PostgresMessageConsumer;
use Fedor\MessageBus\Worker\Queue\QueueLeaseLost;
use Fedor\MessageBus\Worker\Queue\ReceivedQueueMessage;
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;
use RuntimeException;

final class PostgresMessageConsumerIntegrationTest extends TestCase
{
    private PostgresConnectionPool $postgres;
    private QueueTableDefinition $table;
    private PostgresQueueGateway $producer;
    private WorkerFrozenClock $clock;
    private bool $databaseInitialized = false;

    protected function setUp(): void
    {
        $config = getenv('MESSAGE_BUS_TEST_POSTGRES_CONFIG');
        if (!is_string($config) || trim($config) === '') {
            self::markTestSkipped(
                'MESSAGE_BUS_TEST_POSTGRES_CONFIG must contain a libpq connection string.',
            );
        }

        $this->postgres = new PostgresConnectionPool(PostgresConfig::fromString($config));
        $this->table = new QueueTableDefinition('message_bus_test_worker_jobs_' . getmypid());
        $this->databaseInitialized = true;
        $this->postgres->query('DROP TABLE IF EXISTS "' . $this->table->tableName . '"');
        $this->applySchema((new PostgresQueueSchemaGenerator())->generate($this->table));
        $this->producer = new PostgresQueueGateway($this->postgres, $this->table);
        $this->clock = new WorkerFrozenClock(new DateTimeImmutable('2026-11-02T20:00:00+00:00'));
    }

    protected function tearDown(): void
    {
        if (!$this->databaseInitialized) {
            return;
        }

        $this->postgres->query('DROP TABLE IF EXISTS "' . $this->table->tableName . '"');
        $this->postgres->close();
    }

    public function testClaimLeaseAckRetryRejectCancelAndStaleRecovery(): void
    {
        $cancellation = new NullCancellation();
        $consumer = new PostgresMessageConsumer($this->postgres, $this->clock, $this->table);
        $workerA = new ConsumerOptions('postgres', 'default', workerId: 'worker-a', lockTtlSeconds: 300);

        $ackId = $this->enqueue('message-ack');
        $ack = $consumer->next($workerA, $cancellation);
        self::assertNotNull($ack);
        self::assertSame($ackId, $ack->queueMessageId);
        self::assertSame('test.event', $ack->message->envelope->messageName);
        self::assertSame(1, $ack->attempts);
        self::assertSame('worker-a', $ack->leaseOwner);
        $consumer->heartbeat($ack, $cancellation);
        $this->producer->requestCancellation($ackId, $cancellation);
        self::assertTrue($consumer->isCancellationRequested($ack, $cancellation));
        $consumer->ack($ack, $cancellation);
        self::assertSame(QueueJobState::Succeeded, $this->producer->get($ackId, $cancellation)?->status);

        $retryId = $this->enqueue('message-retry');
        $retry = $consumer->next($workerA, $cancellation);
        self::assertNotNull($retry);
        self::assertSame($retryId, $retry->queueMessageId);
        $consumer->retry($retry, new RuntimeException('retry me'), $cancellation);
        $retryStatus = $this->producer->get($retryId, $cancellation);
        self::assertNotNull($retryStatus);
        self::assertSame(QueueJobState::Pending, $retryStatus->status);
        self::assertSame('retry me', $retryStatus->lastError);

        $this->clock->advance(new DateInterval('PT10S'));
        $retried = $consumer->next($workerA, $cancellation);
        self::assertNotNull($retried);
        self::assertSame(2, $retried->attempts);
        $consumer->reject($retried, new RuntimeException('reject me'), $cancellation);
        self::assertSame(QueueJobState::Failed, $this->producer->get($retryId, $cancellation)?->status);

        $cancelId = $this->enqueue('message-cancel');
        $cancelled = $consumer->next($workerA, $cancellation);
        self::assertNotNull($cancelled);
        $consumer->cancel($cancelled, new RuntimeException('cancel me'), $cancellation);
        self::assertSame(QueueJobState::Cancelled, $this->producer->get($cancelId, $cancellation)?->status);

        $leaseId = $this->enqueue('message-lease');
        $leased = $consumer->next($workerA, $cancellation);
        self::assertNotNull($leased);
        try {
            $consumer->ack(new ReceivedQueueMessage(
                $leased->queueMessageId,
                $leased->message,
                $leased->attempts,
                'worker-b',
            ), $cancellation);
            self::fail('A different lease owner must not acknowledge the job.');
        } catch (QueueLeaseLost) {
        }
        $consumer->ack($leased, $cancellation);
        self::assertSame(QueueJobState::Succeeded, $this->producer->get($leaseId, $cancellation)?->status);

        $staleId = $this->enqueue('message-stale');
        $stale = $consumer->next($workerA, $cancellation);
        self::assertNotNull($stale);
        $this->clock->advance(new DateInterval('PT301S'));
        self::assertSame(1, $consumer->recoverStale($workerA, $cancellation));
        self::assertSame(QueueJobState::Pending, $this->producer->get($staleId, $cancellation)?->status);
        $this->clock->advance(new DateInterval('PT10S'));
        $recovered = $consumer->next($workerA, $cancellation);
        self::assertNotNull($recovered);
        self::assertSame('worker-a', $recovered->leaseOwner);
        self::assertSame(2, $recovered->attempts);
        try {
            $consumer->ack($stale, $cancellation);
            self::fail('An earlier attempt must not acknowledge a re-leased job with the same worker ID.');
        } catch (QueueLeaseLost) {
        }
        $consumer->ack($recovered, $cancellation);
    }

    private function enqueue(string $messageId): string
    {
        return $this->producer->enqueue(new QueueMessage(
            'postgres',
            'default',
            new SerializedEnvelope('test.event', '{"value":1}'),
            $messageId,
            'correlation-1',
            'async',
            'binding.one',
            $this->clock->now(),
            retryPolicySnapshot: new RetryPolicySnapshot(3, 'fixed', ['delaySeconds' => 10]),
        ), new NullCancellation())->queueMessageId;
    }

    private function applySchema(string $schema): void
    {
        foreach (explode(";\n", $schema) as $statement) {
            $statement = trim($statement);
            if ($statement !== '') {
                $this->postgres->query($statement);
            }
        }
    }
}

final class WorkerFrozenClock implements ClockInterface
{
    public function __construct(private DateTimeImmutable $now)
    {
    }

    #[\Override]
    public function now(): DateTimeImmutable
    {
        return $this->now;
    }

    public function advance(DateInterval $interval): void
    {
        $this->now = $this->now->add($interval);
    }
}
