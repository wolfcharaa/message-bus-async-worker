<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use DateTimeImmutable;

final readonly class WorkerControlRequest
{
    public function __construct(
        public ?string $createdBy = null,
        public string $source = 'unknown',
        public ?string $reason = null,
        public ?string $requestId = null,
        public ?string $correlationId = null,
        public ?DateTimeImmutable $expiresAt = null,
        public ?string $idempotencyKey = null,
        public bool $override = false,
    ) {
    }
}
