<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Tests;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use DateTimeImmutable;
use Fedor\MessageBus\Worker\Control\DefaultWorkerControlService;
use Fedor\MessageBus\Worker\Control\WorkerControlAcknowledgement;
use Fedor\MessageBus\Worker\Control\WorkerControlCommand;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandReceipt;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandRepositoryInterface;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandType;
use Fedor\MessageBus\Worker\Control\WorkerControlCursor;
use Fedor\MessageBus\Worker\Control\WorkerControlIdGeneratorInterface;
use Fedor\MessageBus\Worker\Control\WorkerControlRequest;
use Fedor\MessageBus\Worker\Control\WorkerDesiredState;
use Fedor\MessageBus\Worker\Control\WorkerDesiredStateRepositoryInterface;
use Fedor\MessageBus\Worker\Control\WorkerDesiredStateType;
use Fedor\MessageBus\Worker\Control\WorkerIdentity;
use Fedor\MessageBus\Worker\Control\WorkerMode;
use Fedor\MessageBus\Worker\Control\WorkerTarget;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;

final class WorkerControlContractsTest extends TestCase
{
    public function testOneShotCommandRequiresExpiryAndRetainsAuditRequest(): void
    {
        try {
            new WorkerControlCommand(
                'command-1',
                WorkerControlCommandType::Kill,
                WorkerTarget::all(),
                new DateTimeImmutable('2026-11-02T20:00:00+00:00'),
            );
            self::fail('Expected one-shot command without expiry to fail.');
        } catch (InvalidArgumentException $exception) {
            self::assertStringContainsString('requires expiresAt', $exception->getMessage());
        }

        $command = WorkerControlCommand::fromRequest(
            'command-2',
            WorkerControlCommandType::Stop,
            new WorkerTarget(workerGroup: 'imports'),
            new DateTimeImmutable('2026-11-02T20:00:00+00:00'),
            new WorkerControlRequest(
                createdBy: 'operator',
                source: 'cli',
                reason: 'deployment',
                requestId: 'request-1',
                correlationId: 'maintenance-1',
                expiresAt: new DateTimeImmutable('2026-11-02T20:05:00+00:00'),
                idempotencyKey: 'stop-imports',
            ),
        );

        self::assertSame('operator', $command->createdBy);
        self::assertSame('cli', $command->source);
        self::assertSame('stop-imports', $command->idempotencyKey);
        self::assertFalse($command->isExpired(new DateTimeImmutable('2026-11-02T20:04:59+00:00')));
        self::assertTrue($command->isExpired(new DateTimeImmutable('2026-11-02T20:05:00+00:00')));
    }

    public function testTargetMatchesV6FiltersAndKeepsSpecificityOrdering(): void
    {
        $identity = self::identity();
        $broad = new WorkerTarget(workerGroup: 'imports');
        $specific = new WorkerTarget(
            workerInstanceId: 'import-worker-1',
            flows: ['gar'],
            bindingPatterns: ['gar.*'],
        );

        self::assertTrue(WorkerTarget::all()->matches($identity));
        self::assertTrue($broad->matches($identity));
        self::assertTrue($specific->matches($identity));
        self::assertFalse((new WorkerTarget(workerGroup: 'other'))->matches($identity));
        self::assertGreaterThan($broad->specificityScore(), $specific->specificityScore());
        self::assertEquals($specific, WorkerTarget::fromArray($specific->toArray()));
    }

    public function testIdentityRoundTripPreservesRoutingSelectors(): void
    {
        $identity = self::identity();

        self::assertEquals($identity, WorkerIdentity::fromArray($identity->toArray()));
    }

    public function testPauseWritesCommandAndDesiredStateWhileDuplicateDoesNotReapply(): void
    {
        $repository = new ControlMemoryRepository();
        $service = new DefaultWorkerControlService(
            $repository,
            $repository,
            new ControlDeterministicIds(),
            new ControlFrozenClock(),
        );
        $target = new WorkerTarget(workerGroup: 'imports');

        $first = $service->pause(
            $target,
            new WorkerControlRequest(
                createdBy: 'operator',
                source: 'cli',
                idempotencyKey: 'pause-imports',
            ),
        );
        $repository->duplicate = true;
        $duplicate = $service->pause(
            $target,
            new WorkerControlRequest(idempotencyKey: 'pause-imports'),
        );

        self::assertSame('command.pause.1', $first->commandId);
        self::assertFalse($first->duplicate);
        self::assertTrue($duplicate->duplicate);
        self::assertCount(2, $repository->commands);
        self::assertCount(1, $repository->desiredStates);
        self::assertSame(WorkerDesiredStateType::Paused, $repository->desiredStates[0]->type);
        self::assertSame('operator', $repository->desiredStates[0]->createdBy);
    }

    public function testOneShotHelpersApplyV6DefaultExpiry(): void
    {
        $repository = new ControlMemoryRepository();
        $service = new DefaultWorkerControlService(
            $repository,
            $repository,
            new ControlDeterministicIds(),
            new ControlFrozenClock(),
        );

        $service->kill(new WorkerTarget(workerInstanceId: 'import-worker-1'));
        $service->stop(new WorkerTarget(workerInstanceId: 'import-worker-2'));

        self::assertSame('2026-11-02T20:01:00+00:00', $repository->commands[0]->expiresAt?->format(DATE_ATOM));
        self::assertSame('2026-11-02T20:05:00+00:00', $repository->commands[1]->expiresAt?->format(DATE_ATOM));
    }

    public function testRepositoryIOPropagatesAmpCancellation(): void
    {
        $repository = new ControlMemoryRepository();
        $service = new DefaultWorkerControlService($repository, $repository);
        $deferred = new DeferredCancellation();
        $deferred->cancel();

        $this->expectException(\Amp\CancelledException::class);

        $service->resume(
            new WorkerTarget(workerGroup: 'imports'),
            cancellation: $deferred->getCancellation(),
        );
    }

    private static function identity(): WorkerIdentity
    {
        return new WorkerIdentity(
            'import-worker',
            'import-worker-1',
            'imports',
            'worker-host',
            123,
            new DateTimeImmutable('2026-11-02T20:00:00+00:00'),
            WorkerMode::Single,
            'postgres',
            'messages',
            ['gar'],
            ['gar.import'],
            ['gar.*'],
            'worker-id-1',
        );
    }
}

final class ControlMemoryRepository implements
    WorkerControlCommandRepositoryInterface,
    WorkerDesiredStateRepositoryInterface
{
    /** @var list<WorkerControlCommand> */
    public array $commands = [];
    /** @var list<WorkerDesiredState> */
    public array $desiredStates = [];
    public bool $duplicate = false;

    public function append(
        WorkerControlCommand $command,
        Cancellation $cancellation,
    ): WorkerControlCommandReceipt {
        $cancellation->throwIfRequested();
        $this->commands[] = $command;

        return new WorkerControlCommandReceipt(
            $command->commandId,
            $command->type,
            $command->target,
            $command->createdAt,
            $command->idempotencyKey,
            $this->duplicate,
        );
    }

    public function findById(string $commandId, Cancellation $cancellation): ?WorkerControlCommand
    {
        return null;
    }

    public function findByIdempotencyKey(
        string $idempotencyKey,
        Cancellation $cancellation,
    ): ?WorkerControlCommand {
        return null;
    }

    public function pendingFor(
        WorkerIdentity $identity,
        WorkerControlCursor $cursor,
        Cancellation $cancellation,
    ): array {
        return [];
    }

    public function acknowledge(
        WorkerControlAcknowledgement $acknowledgement,
        Cancellation $cancellation,
    ): void {
    }

    public function apply(WorkerDesiredState $state, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        $this->desiredStates[] = $state;
    }

    public function resolveFor(
        WorkerIdentity $identity,
        Cancellation $cancellation,
    ): ?WorkerDesiredState {
        return null;
    }

    public function list(WorkerTarget $target, Cancellation $cancellation): array
    {
        return $this->desiredStates;
    }
}

final class ControlDeterministicIds implements WorkerControlIdGeneratorInterface
{
    private int $index = 0;

    public function nextCommandId(WorkerControlCommandType $type): string
    {
        return 'command.' . $type->value . '.' . ++$this->index;
    }

    public function nextDesiredStateId(WorkerControlCommand $command): string
    {
        return 'desired.' . $command->commandId;
    }
}

final readonly class ControlFrozenClock implements ClockInterface
{
    public function now(): DateTimeImmutable
    {
        return new DateTimeImmutable('2026-11-02T20:00:00+00:00');
    }
}
