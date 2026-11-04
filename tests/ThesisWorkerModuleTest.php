<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Tests;

use Amp\Cancellation;
use DateTimeImmutable;
use Fedor\MessageBus\Attribute\QueryHandler;
use Fedor\MessageBus\Envelope\Envelope;
use Fedor\MessageBus\Envelope\EnvelopeSerializerInterface;
use Fedor\MessageBus\Envelope\SerializedEnvelope;
use Fedor\MessageBus\Integration\Thesis\MessageBusModule;
use Fedor\MessageBus\Message\Query;
use Fedor\MessageBus\Queue\QueueMessage;
use Fedor\MessageBus\Registry\CompiledMessageRegistry;
use Fedor\MessageBus\Registry\MessageRegistryCompiler;
use Fedor\MessageBus\Worker\Integration\Thesis\WorkerModule;
use Fedor\MessageBus\Worker\Queue\ConsumerOptions;
use Fedor\MessageBus\Worker\Queue\MessageConsumerInterface;
use Fedor\MessageBus\Worker\Queue\QueueLeaseControlInterface;
use Fedor\MessageBus\Worker\Queue\QueueWorkerRunnerOptions;
use Fedor\MessageBus\Worker\Queue\ReceivedQueueMessage;
use Fedor\MessageBus\Worker\Runtime\WorkerRuntime;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Thesis\Dic;
use Thesis\Dic\Module;
use Thesis\Dic\Ref;
use Throwable;

final class ThesisWorkerModuleTest extends TestCase
{
    public function testCoreAndWorkerModulesShareApplicationServicesAndDisposeTheJobScope(): void
    {
        $trace = new ThesisWorkerModuleTrace();
        $consumer = new ThesisWorkerModuleConsumer();

        $result = Dic::run(
            new ThesisWorkerApplicationModule($trace, $consumer),
            static fn (WorkerRuntime $runtime) => $runtime->runner->run(
                new ConsumerOptions('postgres', 'messages'),
                new QueueWorkerRunnerOptions(maxMessages: 1),
            ),
        );

        self::assertSame(1, $result->handled);
        self::assertSame(1, $result->succeeded);
        self::assertSame(['ack:job-1'], $consumer->transitions);
        self::assertSame(['payload'], $trace->handledValues);
        self::assertSame(1, $trace->created);
        self::assertSame([null], $trace->disposedErrors);
    }
}

/** @implements Query<string> */
final readonly class ThesisWorkerModuleQuery implements Query
{
    public function __construct(public string $value)
    {
    }
}

#[QueryHandler(
    ThesisWorkerModuleQuery::class,
    bindingId: 'worker.module.query',
    contextAware: false,
)]
final readonly class ThesisWorkerModuleQueryHandler
{
    public function __construct(private ThesisWorkerModuleTrace $trace)
    {
        ++$this->trace->created;
    }

    public function __invoke(ThesisWorkerModuleQuery $query): string
    {
        $this->trace->handledValues[] = $query->value;

        return $query->value;
    }

    public function dispose(?Throwable $error): void
    {
        $this->trace->disposedErrors[] = $error;
    }
}

final class ThesisWorkerModuleTrace
{
    public int $created = 0;

    /** @var list<string> */
    public array $handledValues = [];

    /** @var list<?Throwable> */
    public array $disposedErrors = [];
}

final class ThesisWorkerModuleSerializer implements EnvelopeSerializerInterface
{
    public function serialize(Envelope $envelope): SerializedEnvelope
    {
        throw new RuntimeException('Serialization is not used by the worker module test.');
    }

    public function deserialize(SerializedEnvelope $envelope): Envelope
    {
        return new Envelope(
            new ThesisWorkerModuleQuery('payload'),
            'message-1',
            'correlation-1',
            null,
        );
    }
}

final class ThesisWorkerModuleConsumer implements MessageConsumerInterface, QueueLeaseControlInterface
{
    /** @var list<string> */
    public array $transitions = [];

    private bool $claimed = false;

    public function next(ConsumerOptions $options, Cancellation $cancellation): ?ReceivedQueueMessage
    {
        if ($this->claimed) {
            return null;
        }
        $this->claimed = true;

        return new ReceivedQueueMessage(
            'job-1',
            new QueueMessage(
                'postgres',
                'messages',
                new SerializedEnvelope('worker.module.query', '{}'),
                'message-1',
                'correlation-1',
                'default',
                'worker.module.query',
                new DateTimeImmutable('2026-11-04T10:00:00+00:00'),
            ),
            1,
            'worker-1',
        );
    }

    public function ack(ReceivedQueueMessage $message, Cancellation $cancellation): void
    {
        $this->transitions[] = 'ack:' . $message->queueMessageId;
    }

    public function retry(ReceivedQueueMessage $message, Throwable $reason, Cancellation $cancellation): void
    {
        $this->transitions[] = 'retry:' . $message->queueMessageId;
    }

    public function reject(ReceivedQueueMessage $message, Throwable $reason, Cancellation $cancellation): void
    {
        $this->transitions[] = 'reject:' . $message->queueMessageId;
    }

    public function cancel(ReceivedQueueMessage $message, Throwable $reason, Cancellation $cancellation): void
    {
        $this->transitions[] = 'cancel:' . $message->queueMessageId;
    }

    public function heartbeat(ReceivedQueueMessage $message, Cancellation $cancellation): void
    {
    }

    public function isCancellationRequested(ReceivedQueueMessage $message, Cancellation $cancellation): bool
    {
        return false;
    }

    public function recoverStale(ConsumerOptions $options, Cancellation $cancellation): int
    {
        return 0;
    }
}

/** @implements Module<Ref<WorkerRuntime>> */
final readonly class ThesisWorkerApplicationModule implements Module
{
    public function __construct(
        private ThesisWorkerModuleTrace $trace,
        private ThesisWorkerModuleConsumer $consumer,
    ) {
    }

    /** @return Ref<WorkerRuntime> */
    public function configure(Dic $dic): Ref
    {
        $registry = new CompiledMessageRegistry(
            (new MessageRegistryCompiler())->compile([ThesisWorkerModuleQueryHandler::class]),
        );
        $trace = $dic->value($this->trace);
        $handler = $dic
            ->object(ThesisWorkerModuleQueryHandler::class)
            ->arg('trace', $trace)
            ->scoped()
            ->disposer(static function (ThesisWorkerModuleQueryHandler $handler, ?Throwable $error): void {
                $handler->dispose($error);
            });
        $services = [ThesisWorkerModuleQueryHandler::class => $handler];
        $messageBus = $dic->import(new MessageBusModule(
            registry: $dic->value($registry),
            services: $services,
        ));
        $consumer = $dic->value($this->consumer);

        return $dic->import(new WorkerModule(
            consumer: $consumer,
            leaseControl: $consumer,
            executor: $messageBus,
            serializer: $dic->value(new ThesisWorkerModuleSerializer()),
            services: $services,
        ));
    }
}
