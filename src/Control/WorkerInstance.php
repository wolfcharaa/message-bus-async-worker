<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use DateTimeImmutable;

final readonly class WorkerInstance
{
    public function __construct(
        public WorkerIdentity $identity,
        public WorkerLifecycleState $state,
        public WorkerActivityState $activity,
        public DateTimeImmutable $heartbeatAt,
        public int $childrenCount = 0,
        public ?string $lastCommandId = null,
        public ?DateTimeImmutable $lastCommandAt = null,
        public ?DateTimeImmutable $stoppedAt = null,
        public ?string $failureMessage = null,
    ) {
    }
}
