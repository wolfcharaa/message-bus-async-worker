<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use DateTimeImmutable;

final readonly class WorkerChildInstance
{
    public function __construct(
        public string $childInstanceId,
        public string $parentWorkerInstanceId,
        public int $pid,
        public WorkerChildState $state,
        public DateTimeImmutable $startedAt,
        public DateTimeImmutable $heartbeatAt,
        public ?string $queueMessageId = null,
        public ?string $messageId = null,
        public ?string $correlationId = null,
        public ?string $bindingId = null,
        public ?DateTimeImmutable $finishedAt = null,
        public ?string $failureMessage = null,
    ) {
    }
}
