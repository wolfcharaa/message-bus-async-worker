<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Tests;

use DateTimeImmutable;
use Fedor\MessageBus\Envelope\SerializedEnvelope;
use Fedor\MessageBus\Queue\QueueMessage;
use Fedor\MessageBus\Queue\RetryPolicySnapshot;
use Fedor\MessageBus\Worker\Queue\ConsumerOptions;
use Fedor\MessageBus\Worker\Queue\ReceivedQueueMessage;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class ConsumerContractsTest extends TestCase
{
    public function testItPreservesTheV6ConsumerDefaultsAndFilters(): void
    {
        $options = new ConsumerOptions(
            'postgres',
            'default',
            flows: ['async'],
            bindingIds: ['address.one'],
            bindingPatterns: ['address.*'],
        );

        self::assertSame(5, $options->timeoutSeconds);
        self::assertSame(1, $options->limit);
        self::assertSame('message-bus-worker', $options->workerId);
        self::assertSame(300, $options->lockTtlSeconds);
        self::assertSame(['async'], $options->flows);
        self::assertSame(['address.one'], $options->bindingIds);
        self::assertSame(['address.*'], $options->bindingPatterns);
    }

    public function testItRejectsAnInvalidLeaseConfiguration(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('lock TTL must be positive');

        new ConsumerOptions('postgres', 'default', lockTtlSeconds: 0);
    }

    public function testReceivedMessageUsesThePersistedAttemptBudget(): void
    {
        $message = self::message(maxAttempts: 3);

        self::assertTrue((new ReceivedQueueMessage('1', $message, 1, 'worker-a'))->canRetry());
        self::assertTrue((new ReceivedQueueMessage('1', $message, 2, 'worker-a'))->canRetry());
        self::assertFalse((new ReceivedQueueMessage('1', $message, 3, 'worker-a'))->canRetry());
    }

    private static function message(int $maxAttempts): QueueMessage
    {
        return new QueueMessage(
            'postgres',
            'default',
            new SerializedEnvelope('test.event', '{"value":1}'),
            'message-1',
            'correlation-1',
            'async',
            'binding.one',
            new DateTimeImmutable('2026-11-02T20:00:00+00:00'),
            retryPolicySnapshot: new RetryPolicySnapshot(
                $maxAttempts,
                'fixed',
                ['delaySeconds' => 10],
            ),
        );
    }
}
