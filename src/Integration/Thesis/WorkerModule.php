<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Integration\Thesis;

use Fedor\MessageBus\Envelope\EnvelopeSerializerInterface;
use Fedor\MessageBus\Execution\BindingEnvelopeExecutorInterface;
use Fedor\MessageBus\Integration\Thesis\ThesisServiceResolver;
use Fedor\MessageBus\Integration\Thesis\ThesisServiceScopeFactory;
use Fedor\MessageBus\Service\ServiceScopeFactoryInterface;
use Fedor\MessageBus\Worker\Control\WorkerControlRuntime;
use Fedor\MessageBus\Worker\Queue\MessageConsumerInterface;
use Fedor\MessageBus\Worker\Queue\QueueLeaseControlInterface;
use Fedor\MessageBus\Worker\Queue\QueueWorkerRunner;
use Fedor\MessageBus\Worker\Runtime\WorkerRuntime;
use InvalidArgumentException;
use LogicException;
use Psr\Clock\ClockInterface;
use Thesis\Dic;
use Thesis\Dic\Module;
use Thesis\Dic\Ref;

/** @implements Module<Ref<WorkerRuntime>> */
final readonly class WorkerModule implements Module
{
    /**
     * @param Ref<MessageConsumerInterface> $consumer
     * @param Ref<QueueLeaseControlInterface> $leaseControl
     * @param Ref<BindingEnvelopeExecutorInterface>|null $executor
     * @param Ref<EnvelopeSerializerInterface>|null $serializer
     * @param array<string, Ref<object>> $services
     * @param Ref<ServiceScopeFactoryInterface>|null $scopeFactory
     * @param Ref<WorkerControlRuntime>|null $control
     * @param Ref<ClockInterface>|null $clock
     */
    public function __construct(
        private Ref $consumer,
        private Ref $leaseControl,
        private ?Ref $executor = null,
        private ?Ref $serializer = null,
        private array $services = [],
        private ?Ref $scopeFactory = null,
        private ?Ref $control = null,
        private ?Ref $clock = null,
    ) {
        if ($this->scopeFactory === null && ($this->executor === null || $this->serializer === null)) {
            throw new InvalidArgumentException(
                'WorkerModule requires executor and serializer when no explicit scope factory is supplied.',
            );
        }
        if (
            $this->scopeFactory !== null
            && ($this->executor !== null || $this->serializer !== null || $this->services !== [])
        ) {
            throw new InvalidArgumentException(
                'WorkerModule executor, serializer, and services cannot be combined with an explicit scope factory.',
            );
        }
    }

    /** @return Ref<WorkerRuntime> */
    public function configure(Dic $dic): Ref
    {
        $runnerArguments = [
            'consumer' => $this->consumer,
            'leaseControl' => $this->leaseControl,
            'scopeFactory' => $this->scopeFactory ?? $this->defaultScopeFactory($dic),
        ];
        if ($this->control !== null) {
            $runnerArguments['workerControlRuntime'] = $this->control;
        }
        if ($this->clock !== null) {
            $runnerArguments['clock'] = $this->clock;
        }

        $runner = $dic
            ->object(QueueWorkerRunner::class)
            ->args($runnerArguments);
        $runtimeArguments = ['runner' => $runner];
        if ($this->control !== null) {
            $runtimeArguments['control'] = $this->control;
        }

        return $dic
            ->object(WorkerRuntime::class)
            ->args($runtimeArguments);
    }

    /** @return Ref<ThesisServiceScopeFactory> */
    private function defaultScopeFactory(Dic $dic): Ref
    {
        $executor = $this->executor
            ?? throw new LogicException('WorkerModule executor was validated during construction.');
        $serializer = $this->serializer
            ?? throw new LogicException('WorkerModule serializer was validated during construction.');
        $providers = [];
        foreach ($this->services as $id => $service) {
            $providers[$id] = $dic->provider($service);
        }

        $services = $dic
            ->object(ThesisServiceResolver::class)
            ->arg('providers', $dic->value($providers))
            ->scoped();
        $jobServices = $dic
            ->object(MessageBusWorkerServiceResolver::class)
            ->args([
                'services' => $services,
                'executor' => $executor,
                'serializer' => $serializer,
            ])
            ->scoped();

        return $dic
            ->object(ThesisServiceScopeFactory::class)
            ->arg('resolver', $dic->scoped($jobServices));
    }
}
