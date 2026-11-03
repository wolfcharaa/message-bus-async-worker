<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use Amp\Cancellation;
use Amp\NullCancellation;

interface WorkerStatusServiceInterface
{
    public function getWorker(
        string $workerInstanceId,
        Cancellation $cancellation = new NullCancellation(),
    ): ?WorkerInstance;

    /** @return list<WorkerInstance> */
    public function listWorkers(
        WorkerTarget $target,
        Cancellation $cancellation = new NullCancellation(),
    ): array;

    /** @return list<WorkerChildInstance> */
    public function listChildren(
        string $workerInstanceId,
        Cancellation $cancellation = new NullCancellation(),
    ): array;

    /** @return list<WorkerControlAcknowledgement> */
    public function acknowledgementsForCommand(
        string $commandId,
        Cancellation $cancellation = new NullCancellation(),
    ): array;
}
