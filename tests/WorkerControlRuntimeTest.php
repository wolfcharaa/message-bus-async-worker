<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Tests;

use Amp\NullCancellation;
use Fedor\MessageBus\Worker\Control\DefaultWorkerStatusService;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandRepositoryInterface;
use Fedor\MessageBus\Worker\Control\WorkerControlInboxInterface;
use Fedor\MessageBus\Worker\Control\WorkerControlRuntime;
use Fedor\MessageBus\Worker\Control\WorkerControlServiceInterface;
use Fedor\MessageBus\Worker\Control\WorkerDesiredStateRepositoryInterface;
use Fedor\MessageBus\Worker\Control\WorkerRegistryInterface;
use Fedor\MessageBus\Worker\Control\WorkerStatusRepositoryInterface;
use PHPUnit\Framework\TestCase;

final class WorkerControlRuntimeTest extends TestCase
{
    public function testRuntimeKeepsV6CompositionBoundaryAndBuildsStatusFacade(): void
    {
        $controlService = $this->createStub(WorkerControlServiceInterface::class);
        $commands = $this->createStub(WorkerControlCommandRepositoryInterface::class);
        $desiredStates = $this->createStub(WorkerDesiredStateRepositoryInterface::class);
        $registry = $this->createStub(WorkerRegistryInterface::class);
        $statuses = $this->createStub(WorkerStatusRepositoryInterface::class);
        $inbox = $this->createStub(WorkerControlInboxInterface::class);

        $runtime = new WorkerControlRuntime(
            $controlService,
            $commands,
            $desiredStates,
            $registry,
            $statuses,
            $inbox,
        );

        self::assertSame($controlService, $runtime->controlService);
        self::assertSame($commands, $runtime->commandRepository);
        self::assertSame($desiredStates, $runtime->desiredStateRepository);
        self::assertSame($registry, $runtime->workerRegistry);
        self::assertSame($statuses, $runtime->statusRepository);
        self::assertSame($inbox, $runtime->inbox);
        self::assertInstanceOf(DefaultWorkerStatusService::class, $runtime->statusService);
    }

    public function testStatusFacadePropagatesCancellationToRepository(): void
    {
        $cancellation = new NullCancellation();
        $repository = $this->createMock(WorkerStatusRepositoryInterface::class);
        $repository->expects(self::once())
            ->method('getWorker')
            ->with('worker-1', $cancellation)
            ->willReturn(null);

        $service = new DefaultWorkerStatusService($repository);

        self::assertNull($service->getWorker('worker-1', $cancellation));
    }
}
