<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use Amp\Cancellation;

interface WorkerStatusRepositoryInterface
{
    public function getWorker(string $workerInstanceId, Cancellation $cancellation): ?WorkerInstance;

    /** @return list<WorkerInstance> */
    public function listWorkers(WorkerTarget $target, Cancellation $cancellation): array;

    /** @return list<WorkerChildInstance> */
    public function listChildren(string $workerInstanceId, Cancellation $cancellation): array;

    /** @return list<WorkerControlAcknowledgement> */
    public function acknowledgementsForCommand(
        string $commandId,
        Cancellation $cancellation,
    ): array;
}
