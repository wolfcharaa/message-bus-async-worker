<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use DateTimeImmutable;

final readonly class WorkerControlCommandReceipt
{
    public function __construct(
        public string $commandId,
        public WorkerControlCommandType $type,
        public WorkerTarget $target,
        public DateTimeImmutable $acceptedAt,
        public ?string $idempotencyKey = null,
        public bool $duplicate = false,
    ) {
    }
}
