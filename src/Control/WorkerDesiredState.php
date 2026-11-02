<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use DateTimeImmutable;

final readonly class WorkerDesiredState
{
    public function __construct(
        public string $desiredStateId,
        public WorkerDesiredStateType $type,
        public WorkerTarget $target,
        public DateTimeImmutable $createdAt,
        public ?string $createdBy = null,
        public string $source = 'unknown',
        public ?string $reason = null,
        public ?string $requestId = null,
        public ?string $correlationId = null,
        public bool $override = false,
        public bool $active = true,
    ) {
    }

    public function matches(WorkerIdentity $identity): bool
    {
        return $this->active && $this->target->matches($identity);
    }

    public function specificityScore(): int
    {
        return $this->target->specificityScore();
    }
}
