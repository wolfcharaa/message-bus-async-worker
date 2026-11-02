<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Tests;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Amp\NullCancellation;
use Closure;
use DateTimeImmutable;
use Fedor\MessageBus\Envelope\SerializedEnvelope;
use Fedor\MessageBus\Exception\NonRetryableMessageExceptionInterface;
use Fedor\MessageBus\Queue\QueueMessage;
use Fedor\MessageBus\Queue\RetryPolicySnapshot;
use Fedor\MessageBus\Service\ServiceReference;
use Fedor\MessageBus\Service\ServiceScopeFactoryInterface;
use Fedor\MessageBus\Service\ServiceScopeInterface;
use Fedor\MessageBus\Worker\Queue\ConsumerOptions;
use Fedor\MessageBus\Worker\Queue\MessageConsumerInterface;
use Fedor\MessageBus\Worker\Queue\QueueLeaseControlInterface;
use Fedor\MessageBus\Worker\Queue\QueueLeaseLost;
use Fedor\MessageBus\Worker\Queue\QueueWorkerInterface;
use Fedor\MessageBus\Worker\Queue\QueueWorkerRunner;
use Fedor\MessageBus\Worker\Queue\QueueWorkerRunnerOptions;
use Fedor\MessageBus\Worker\Queue\ReceivedQueueMessage;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;

use function Amp\async;
use function Amp\delay;

final class QueueWorkerRunnerTest extends TestCase
{
    public function testItOpensOneScopeHeartbeatsAndAcknowledgesSuccess(): void
    {
        $consumer = new RunnerConsumer([self::message('job-1')]);
        $lease = new RunnerLeaseControl();
        $worker = new RunnerWorker(static function (
            ReceivedQueueMessage $message,
            Cancellation $cancellation,
        ): void {
            delay(0.03, cancellation: $cancellation);
        });
        $scopes = new RunnerScopeFactory($worker);

        $result = (new QueueWorkerRunner($consumer, $lease, $scopes))->run(
            self::consumerOptions(),
            self::runnerOptions(maxMessages: 1),
        );

        self::assertSame(1, $result->handled);
        self::assertSame(1, $result->succeeded);
        self::assertSame(['ack:job-1'], $consumer->transitions);
        self::assertGreaterThanOrEqual(1, $lease->heartbeats);
        self::assertSame(1, $scopes->opened);
        self::assertSame(1, $scopes->disposed);
        self::assertSame([null], $scopes->disposeErrors);
    }

    public function testItClassifiesNonRetryableRetryableAndExhaustedFailures(): void
    {
        $consumer = new RunnerConsumer([
            self::message('job-reject'),
            self::message('job-retry'),
            self::message('job-exhausted', attempts: 3),
        ]);
        $worker = new RunnerWorker(static function (ReceivedQueueMessage $message): void {
            throw $message->queueMessageId === 'job-reject'
                ? new RunnerNonRetryableFailure('reject')
                : new RuntimeException('retry');
        });

        $result = (new QueueWorkerRunner(
            $consumer,
            new RunnerLeaseControl(),
            new RunnerScopeFactory($worker),
        ))->run(self::consumerOptions(), self::runnerOptions(maxMessages: 3));

        self::assertSame(3, $result->handled);
        self::assertSame(1, $result->retried);
        self::assertSame(2, $result->rejected);
        self::assertSame([
            'reject:job-reject',
            'retry:job-retry',
            'reject:job-exhausted',
        ], $consumer->transitions);
    }

    public function testDurableCancellationCancelsTheJobAndPersistsTransition(): void
    {
        $consumer = new RunnerConsumer([self::message('job-cancel')]);
        $lease = new RunnerLeaseControl(cancellationRequested: true);
        $worker = new RunnerWorker(static function (
            ReceivedQueueMessage $message,
            Cancellation $cancellation,
        ): void {
            delay(1, cancellation: $cancellation);
        });

        $result = (new QueueWorkerRunner(
            $consumer,
            $lease,
            new RunnerScopeFactory($worker),
        ))->run(self::consumerOptions(), self::runnerOptions(maxMessages: 1));

        self::assertSame(1, $result->cancelled);
        self::assertSame(['cancel:job-cancel'], $consumer->transitions);
        self::assertGreaterThanOrEqual(1, $lease->cancellationPolls);
    }

    public function testProcessCancellationStopsClaimingButLetsCurrentJobFinishWithinGrace(): void
    {
        $consumer = new RunnerConsumer([
            self::message('job-current'),
            self::message('job-must-not-start'),
        ]);
        $processCancellation = new DeferredCancellation();
        async(static function () use ($processCancellation): void {
            delay(0.005);
            $processCancellation->cancel();
        })->ignore();
        $worker = new RunnerWorker(static function (
            ReceivedQueueMessage $message,
            Cancellation $cancellation,
        ): void {
            delay(0.03, cancellation: $cancellation);
        });

        $result = (new QueueWorkerRunner(
            $consumer,
            new RunnerLeaseControl(),
            new RunnerScopeFactory($worker),
        ))->run(
            self::consumerOptions(),
            self::runnerOptions(maxMessages: 2),
            $processCancellation->getCancellation(),
        );

        self::assertSame(1, $result->handled);
        self::assertSame(1, $result->succeeded);
        self::assertSame(['ack:job-current'], $consumer->transitions);
        self::assertSame(1, $consumer->claims);
    }

    public function testLeaseLossCancelsHandlerWithoutWritingAnotherTransition(): void
    {
        $consumer = new RunnerConsumer([self::message('job-lost')]);
        $lease = new RunnerLeaseControl(loseLeaseOnHeartbeat: true);
        $worker = new RunnerWorker(static function (
            ReceivedQueueMessage $message,
            Cancellation $cancellation,
        ): void {
            delay(1, cancellation: $cancellation);
        });

        $result = (new QueueWorkerRunner(
            $consumer,
            $lease,
            new RunnerScopeFactory($worker),
        ))->run(self::consumerOptions(), self::runnerOptions(maxMessages: 1));

        self::assertSame(1, $result->handled);
        self::assertSame(1, $result->leaseLost);
        self::assertSame([], $consumer->transitions);
    }

    public function testGracefulTimeoutCancelsAndRetriesTheCurrentJob(): void
    {
        $consumer = new RunnerConsumer([self::message('job-timeout')]);
        $processCancellation = new DeferredCancellation();
        async(static function () use ($processCancellation): void {
            delay(0.005);
            $processCancellation->cancel();
        })->ignore();
        $worker = new RunnerWorker(static function (
            ReceivedQueueMessage $message,
            Cancellation $cancellation,
        ): void {
            delay(2, cancellation: $cancellation);
        });

        $result = (new QueueWorkerRunner(
            $consumer,
            new RunnerLeaseControl(),
            new RunnerScopeFactory($worker),
        ))->run(
            self::consumerOptions(),
            self::runnerOptions(maxMessages: 1),
            $processCancellation->getCancellation(),
        );

        self::assertSame(1, $result->retried);
        self::assertSame(['retry:job-timeout'], $consumer->transitions);
    }

    public function testEmptyQueueAndOptionValidationAreDeterministic(): void
    {
        $consumer = new RunnerConsumer([]);
        $result = (new QueueWorkerRunner(
            $consumer,
            new RunnerLeaseControl(),
            new RunnerScopeFactory(new RunnerWorker(static function (): void {})),
        ))->run(
            self::consumerOptions(),
            new QueueWorkerRunnerOptions(stopWhenEmpty: true),
        );

        self::assertSame(0, $result->handled);
        self::assertSame(1, $consumer->claims);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Heartbeat interval must be positive.');

        new QueueWorkerRunnerOptions(heartbeatIntervalMilliseconds: 0);
    }

    private static function consumerOptions(): ConsumerOptions
    {
        return new ConsumerOptions('postgres', 'messages', workerId: 'worker-1');
    }

    private static function runnerOptions(int $maxMessages): QueueWorkerRunnerOptions
    {
        return new QueueWorkerRunnerOptions(
            maxMessages: $maxMessages,
            heartbeatIntervalMilliseconds: 5,
            cancellationPollIntervalMilliseconds: 5,
            gracefulShutdownTimeoutSeconds: 1,
        );
    }

    private static function message(string $id, int $attempts = 1): ReceivedQueueMessage
    {
        return new ReceivedQueueMessage(
            $id,
            new QueueMessage(
                'postgres',
                'messages',
                new SerializedEnvelope('runner.message', '{"id":"' . $id . '"}'),
                'message-' . $id,
                'correlation-1',
                'async',
                'runner.handle',
                new DateTimeImmutable('2026-11-02T20:00:00+00:00'),
                retryPolicySnapshot: new RetryPolicySnapshot(maxAttempts: 3),
            ),
            $attempts,
            'worker-1',
        );
    }
}

final class RunnerConsumer implements MessageConsumerInterface
{
    /** @var list<ReceivedQueueMessage> */
    private array $messages;

    /** @var list<string> */
    public array $transitions = [];
    public int $claims = 0;

    /** @param list<ReceivedQueueMessage> $messages */
    public function __construct(array $messages)
    {
        $this->messages = $messages;
    }

    public function next(ConsumerOptions $options, Cancellation $cancellation): ?ReceivedQueueMessage
    {
        $cancellation->throwIfRequested();
        ++$this->claims;

        return array_shift($this->messages);
    }

    public function ack(ReceivedQueueMessage $message, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        $this->transitions[] = 'ack:' . $message->queueMessageId;
    }

    public function retry(ReceivedQueueMessage $message, Throwable $reason, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        $this->transitions[] = 'retry:' . $message->queueMessageId;
    }

    public function reject(ReceivedQueueMessage $message, Throwable $reason, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        $this->transitions[] = 'reject:' . $message->queueMessageId;
    }

    public function cancel(ReceivedQueueMessage $message, Throwable $reason, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        $this->transitions[] = 'cancel:' . $message->queueMessageId;
    }
}

final class RunnerLeaseControl implements QueueLeaseControlInterface
{
    public int $heartbeats = 0;
    public int $cancellationPolls = 0;

    public function __construct(
        private readonly bool $cancellationRequested = false,
        private readonly bool $loseLeaseOnHeartbeat = false,
    ) {
    }

    public function heartbeat(ReceivedQueueMessage $message, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        ++$this->heartbeats;
        if ($this->loseLeaseOnHeartbeat) {
            throw QueueLeaseLost::forMessage($message);
        }
    }

    public function isCancellationRequested(ReceivedQueueMessage $message, Cancellation $cancellation): bool
    {
        $cancellation->throwIfRequested();
        ++$this->cancellationPolls;

        return $this->cancellationRequested;
    }

    public function recoverStale(ConsumerOptions $options, Cancellation $cancellation): int
    {
        return 0;
    }
}

final readonly class RunnerWorker implements QueueWorkerInterface
{
    /** @var Closure(ReceivedQueueMessage, Cancellation): void */
    private Closure $handler;

    /** @param callable(ReceivedQueueMessage, Cancellation): void $handler */
    public function __construct(callable $handler)
    {
        $this->handler = Closure::fromCallable($handler);
    }

    public function handle(ReceivedQueueMessage $message, Cancellation $cancellation): void
    {
        ($this->handler)($message, $cancellation);
    }
}

final class RunnerScopeFactory implements ServiceScopeFactoryInterface
{
    public int $opened = 0;
    public int $disposed = 0;
    /** @var list<Throwable|null> */
    public array $disposeErrors = [];

    public function __construct(private readonly QueueWorkerInterface $worker)
    {
    }

    public function openScope(): ServiceScopeInterface
    {
        ++$this->opened;

        return new RunnerScope($this);
    }

    public function worker(): QueueWorkerInterface
    {
        return $this->worker;
    }
}

final readonly class RunnerScope implements ServiceScopeInterface
{
    public function __construct(private RunnerScopeFactory $factory)
    {
    }

    public function resolve(ServiceReference $reference): object
    {
        $worker = $this->factory->worker();
        if (!$worker instanceof $reference->type) {
            throw new RuntimeException('Unexpected runner service type.');
        }

        return $worker;
    }

    public function dispose(?Throwable $error = null): void
    {
        ++$this->factory->disposed;
        $this->factory->disposeErrors[] = $error;
    }
}

final class RunnerNonRetryableFailure extends RuntimeException implements NonRetryableMessageExceptionInterface
{
}
