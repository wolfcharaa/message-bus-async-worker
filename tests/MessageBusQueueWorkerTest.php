<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Tests;

use Amp\Cancellation;
use Amp\NullCancellation;
use DateTimeImmutable;
use Fedor\MessageBus\Envelope\Envelope;
use Fedor\MessageBus\Envelope\EnvelopeSerializerInterface;
use Fedor\MessageBus\Envelope\SerializedEnvelope;
use Fedor\MessageBus\Execution\BindingEnvelopeExecutorInterface;
use Fedor\MessageBus\Message\Event;
use Fedor\MessageBus\Queue\QueueMessage;
use Fedor\MessageBus\Service\ServiceReference;
use Fedor\MessageBus\Service\ServiceResolverInterface;
use Fedor\MessageBus\Worker\Queue\MessageBusQueueWorker;
use Fedor\MessageBus\Worker\Queue\ReceivedQueueMessage;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class MessageBusQueueWorkerTest extends TestCase
{
    public function testItDeserializesAndExecutesOnlyTheBindingStoredInQueueRow(): void
    {
        $serialized = new SerializedEnvelope('worker.event', '{"value":1}');
        $envelope = new Envelope(
            new QueueWorkerEvent(),
            'message-1',
            'correlation-1',
            null,
        );
        $serializer = new QueueWorkerSerializer($serialized, $envelope);
        $executor = new QueueWorkerBindingExecutor();
        $services = new QueueWorkerServiceResolver();
        $cancellation = new NullCancellation();
        $worker = new MessageBusQueueWorker($executor, $serializer, $services);

        $worker->handle(new ReceivedQueueMessage(
            'queue-1',
            new QueueMessage(
                'postgres',
                'default',
                $serialized,
                'message-1',
                'correlation-1',
                'async',
                'worker.event.handle',
                new DateTimeImmutable('2026-11-03T10:00:00+00:00'),
            ),
            1,
            'worker-1',
        ), $cancellation);

        self::assertSame(1, $serializer->deserializations);
        self::assertNotNull($executor->envelope);
        self::assertSame('worker.event.handle', $executor->envelope->bindingId);
        self::assertSame('message-1', $executor->envelope->messageId);
        self::assertSame($services, $executor->services);
        self::assertSame($cancellation, $executor->cancellation);
    }
}

final readonly class QueueWorkerEvent implements Event
{
}

final class QueueWorkerSerializer implements EnvelopeSerializerInterface
{
    public int $deserializations = 0;

    public function __construct(
        private readonly SerializedEnvelope $expected,
        private readonly Envelope $envelope,
    ) {
    }

    public function serialize(Envelope $envelope): SerializedEnvelope
    {
        throw new RuntimeException('Serialization is not expected in worker adapter test.');
    }

    public function deserialize(SerializedEnvelope $envelope): Envelope
    {
        Assert::assertSame($this->expected, $envelope);
        ++$this->deserializations;

        return $this->envelope;
    }
}

final class QueueWorkerBindingExecutor implements BindingEnvelopeExecutorInterface
{
    public ?Envelope $envelope = null;
    public ?ServiceResolverInterface $services = null;
    public ?Cancellation $cancellation = null;

    public function executeEnvelopeToBinding(
        Envelope $envelope,
        ServiceResolverInterface $services,
        Cancellation $cancellation,
    ): mixed {
        $this->envelope = $envelope;
        $this->services = $services;
        $this->cancellation = $cancellation;

        return null;
    }
}

final class QueueWorkerServiceResolver implements ServiceResolverInterface
{
    public function resolve(ServiceReference $reference): object
    {
        throw new RuntimeException('Service resolution is delegated to the binding executor.');
    }
}
