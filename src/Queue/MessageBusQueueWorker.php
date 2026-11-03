<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use Amp\Cancellation;
use Fedor\MessageBus\Envelope\EnvelopeSerializerInterface;
use Fedor\MessageBus\Execution\BindingEnvelopeExecutorInterface;
use Fedor\MessageBus\Service\ServiceResolverInterface;

final readonly class MessageBusQueueWorker implements QueueWorkerInterface
{
    public function __construct(
        private BindingEnvelopeExecutorInterface $executor,
        private EnvelopeSerializerInterface $serializer,
        private ServiceResolverInterface $services,
    ) {
    }

    #[\Override]
    public function handle(ReceivedQueueMessage $message, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        $envelope = $this->serializer
            ->deserialize($message->message->envelope)
            ->forBinding($message->message->bindingId);

        $this->executor->executeEnvelopeToBinding($envelope, $this->services, $cancellation);
    }
}
