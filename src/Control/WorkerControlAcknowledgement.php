<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use DateTimeImmutable;

final readonly class WorkerControlAcknowledgement
{
    public function __construct(
        public string $commandId,
        public string $workerInstanceId,
        public WorkerControlAcknowledgementState $state,
        public DateTimeImmutable $acknowledgedAt,
        public ?string $message = null,
        public ?string $errorClass = null,
        public ?string $errorMessage = null,
    ) {
    }
}
