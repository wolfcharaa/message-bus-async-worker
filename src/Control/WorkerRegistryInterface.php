<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use Amp\Cancellation;
use DateTimeImmutable;

interface WorkerRegistryInterface
{
    public function registerWorker(WorkerInstance $worker, Cancellation $cancellation): void;

    public function heartbeatWorker(
        string $workerInstanceId,
        WorkerLifecycleState $state,
        WorkerActivityState $activity,
        DateTimeImmutable $heartbeatAt,
        Cancellation $cancellation,
        int $childrenCount = 0,
        ?string $lastCommandId = null,
    ): void;

    public function stopWorker(
        string $workerInstanceId,
        WorkerLifecycleState $state,
        DateTimeImmutable $stoppedAt,
        Cancellation $cancellation,
        ?string $failureMessage = null,
    ): void;

    public function registerChild(WorkerChildInstance $child, Cancellation $cancellation): void;

    public function heartbeatChild(
        string $childInstanceId,
        WorkerChildState $state,
        DateTimeImmutable $heartbeatAt,
        Cancellation $cancellation,
    ): void;

    public function finishChild(
        string $childInstanceId,
        WorkerChildState $state,
        DateTimeImmutable $finishedAt,
        Cancellation $cancellation,
        ?string $failureMessage = null,
    ): void;
}
