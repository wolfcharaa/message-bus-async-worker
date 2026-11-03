<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

final readonly class WorkerControlRuntime
{
    public WorkerStatusServiceInterface $statusService;

    public function __construct(
        public WorkerControlServiceInterface $controlService,
        public WorkerControlCommandRepositoryInterface $commandRepository,
        public WorkerDesiredStateRepositoryInterface $desiredStateRepository,
        public WorkerRegistryInterface $workerRegistry,
        public WorkerStatusRepositoryInterface $statusRepository,
        public WorkerControlInboxInterface $inbox,
        ?WorkerStatusServiceInterface $statusService = null,
    ) {
        $this->statusService = $statusService ?? new DefaultWorkerStatusService($statusRepository);
    }
}
