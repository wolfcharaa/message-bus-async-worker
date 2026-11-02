<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use DateTimeImmutable;
use InvalidArgumentException;

final readonly class WorkerControlCommand
{
    public function __construct(
        public string $commandId,
        public WorkerControlCommandType $type,
        public WorkerTarget $target,
        public DateTimeImmutable $createdAt,
        public ?string $createdBy = null,
        public string $source = 'unknown',
        public ?string $reason = null,
        public ?string $requestId = null,
        public ?string $correlationId = null,
        public ?DateTimeImmutable $expiresAt = null,
        public ?string $idempotencyKey = null,
        public bool $override = false,
    ) {
        if ($this->type->isOneShot() && $this->expiresAt === null) {
            throw new InvalidArgumentException(sprintf(
                'Worker control command `%s` requires expiresAt.',
                $this->type->value,
            ));
        }
    }

    public static function fromRequest(
        string $commandId,
        WorkerControlCommandType $type,
        WorkerTarget $target,
        DateTimeImmutable $createdAt,
        ?WorkerControlRequest $request = null,
    ): self {
        $request ??= new WorkerControlRequest();

        return new self(
            $commandId,
            $type,
            $target,
            $createdAt,
            $request->createdBy,
            $request->source,
            $request->reason,
            $request->requestId,
            $request->correlationId,
            $request->expiresAt,
            $request->idempotencyKey,
            $request->override,
        );
    }

    public function isExpired(DateTimeImmutable $now): bool
    {
        return $this->expiresAt !== null && $this->expiresAt <= $now;
    }
}
