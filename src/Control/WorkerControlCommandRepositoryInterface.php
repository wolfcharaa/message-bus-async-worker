<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use Amp\Cancellation;

interface WorkerControlCommandRepositoryInterface
{
    public function append(
        WorkerControlCommand $command,
        Cancellation $cancellation,
    ): WorkerControlCommandReceipt;

    public function findById(string $commandId, Cancellation $cancellation): ?WorkerControlCommand;

    public function findByIdempotencyKey(
        string $idempotencyKey,
        Cancellation $cancellation,
    ): ?WorkerControlCommand;

    /** @return list<WorkerControlCommand> */
    public function pendingFor(
        WorkerIdentity $identity,
        WorkerControlCursor $cursor,
        Cancellation $cancellation,
    ): array;

    public function acknowledge(
        WorkerControlAcknowledgement $acknowledgement,
        Cancellation $cancellation,
    ): void;
}
