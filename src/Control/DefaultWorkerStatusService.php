<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use Amp\Cancellation;
use Amp\NullCancellation;

final readonly class DefaultWorkerStatusService implements WorkerStatusServiceInterface
{
    public function __construct(private WorkerStatusRepositoryInterface $repository)
    {
    }

    #[\Override]
    public function getWorker(
        string $workerInstanceId,
        Cancellation $cancellation = new NullCancellation(),
    ): ?WorkerInstance {
        return $this->repository->getWorker($workerInstanceId, $cancellation);
    }

    #[\Override]
    public function listWorkers(
        WorkerTarget $target,
        Cancellation $cancellation = new NullCancellation(),
    ): array {
        return $this->repository->listWorkers($target, $cancellation);
    }

    #[\Override]
    public function listChildren(
        string $workerInstanceId,
        Cancellation $cancellation = new NullCancellation(),
    ): array {
        return $this->repository->listChildren($workerInstanceId, $cancellation);
    }

    #[\Override]
    public function acknowledgementsForCommand(
        string $commandId,
        Cancellation $cancellation = new NullCancellation(),
    ): array {
        return $this->repository->acknowledgementsForCommand($commandId, $cancellation);
    }
}
