<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Integration\Thesis;

use Fedor\MessageBus\Envelope\EnvelopeSerializerInterface;
use Fedor\MessageBus\Execution\BindingEnvelopeExecutorInterface;
use Fedor\MessageBus\Service\ServiceReference;
use Fedor\MessageBus\Service\ServiceResolutionFailed;
use Fedor\MessageBus\Service\ServiceResolverInterface;
use Fedor\MessageBus\Worker\Queue\MessageBusQueueWorker;
use Fedor\MessageBus\Worker\Queue\QueueWorkerInterface;

final class MessageBusWorkerServiceResolver implements ServiceResolverInterface
{
    private ?QueueWorkerInterface $worker = null;

    public function __construct(
        private readonly ServiceResolverInterface $services,
        private readonly BindingEnvelopeExecutorInterface $executor,
        private readonly EnvelopeSerializerInterface $serializer,
    ) {
    }

    public function resolve(ServiceReference $reference): object
    {
        if ($reference->id !== QueueWorkerInterface::class) {
            return $this->services->resolve($reference);
        }

        $worker = $this->worker ??= new MessageBusQueueWorker(
            $this->executor,
            $this->serializer,
            $this,
        );
        if (!$worker instanceof $reference->type) {
            throw ServiceResolutionFailed::typeMismatch($reference, $worker);
        }

        return $worker;
    }
}
