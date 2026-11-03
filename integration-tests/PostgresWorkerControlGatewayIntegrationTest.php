<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\IntegrationTests;

use Amp\NullCancellation;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use DateTimeImmutable;
use Fedor\MessageBus\Worker\Control\Postgres\PostgresWorkerControlGateway;
use Fedor\MessageBus\Worker\Control\Postgres\PostgresWorkerControlSchemaGenerator;
use Fedor\MessageBus\Worker\Control\WorkerActivityState;
use Fedor\MessageBus\Worker\Control\WorkerChildInstance;
use Fedor\MessageBus\Worker\Control\WorkerChildState;
use Fedor\MessageBus\Worker\Control\WorkerControlAcknowledgement;
use Fedor\MessageBus\Worker\Control\WorkerControlAcknowledgementState;
use Fedor\MessageBus\Worker\Control\WorkerControlCommand;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandType;
use Fedor\MessageBus\Worker\Control\WorkerControlCursor;
use Fedor\MessageBus\Worker\Control\WorkerControlTableDefinition;
use Fedor\MessageBus\Worker\Control\WorkerDesiredState;
use Fedor\MessageBus\Worker\Control\WorkerDesiredStateType;
use Fedor\MessageBus\Worker\Control\WorkerIdentity;
use Fedor\MessageBus\Worker\Control\WorkerInstance;
use Fedor\MessageBus\Worker\Control\WorkerLifecycleState;
use Fedor\MessageBus\Worker\Control\WorkerMode;
use Fedor\MessageBus\Worker\Control\WorkerTarget;
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;

final class PostgresWorkerControlGatewayIntegrationTest extends TestCase
{
    private PostgresConnectionPool $postgres;
    private WorkerControlTableDefinition $tables;
    private PostgresWorkerControlGateway $gateway;
    private DateTimeImmutable $now;
    private bool $databaseInitialized = false;

    protected function setUp(): void
    {
        $config = getenv('MESSAGE_BUS_TEST_POSTGRES_CONFIG');
        if (!is_string($config) || trim($config) === '') {
            self::markTestSkipped(
                'MESSAGE_BUS_TEST_POSTGRES_CONFIG must contain a libpq connection string.',
            );
        }

        $this->postgres = new PostgresConnectionPool(PostgresConfig::fromString($config));
        $suffix = (string) getmypid();
        $this->tables = new WorkerControlTableDefinition(
            commandsTable: 'mb_wc_commands_' . $suffix,
            desiredStatesTable: 'mb_wc_states_' . $suffix,
            workerInstancesTable: 'mb_wc_workers_' . $suffix,
            childInstancesTable: 'mb_wc_children_' . $suffix,
            acknowledgementsTable: 'mb_wc_acks_' . $suffix,
            commandDeliveriesTable: 'mb_wc_deliveries_' . $suffix,
            commandAuditTable: 'mb_wc_audit_' . $suffix,
        );
        $this->dropTables();
        $this->applySchema((new PostgresWorkerControlSchemaGenerator())->generate($this->tables));
        $this->databaseInitialized = true;
        $this->now = new DateTimeImmutable('2026-11-02T20:00:00+00:00');
        $this->gateway = new PostgresWorkerControlGateway(
            $this->postgres,
            new WorkerControlFrozenClock($this->now),
            $this->tables,
        );
    }

    protected function tearDown(): void
    {
        if (!$this->databaseInitialized) {
            return;
        }

        $this->dropTables();
        $this->postgres->close();
    }

    public function testCommandInboxKeepsV6IdempotencyTargetingAndCursorSemantics(): void
    {
        $cancellation = new NullCancellation();
        $identity = $this->identity();
        $first = $this->command(
            'command-1',
            new WorkerTarget(workerGroup: 'imports'),
            'deployment-1',
        );

        $receipt = $this->gateway->append($first, $cancellation);
        $duplicate = $this->gateway->append($this->command(
            'command-duplicate',
            new WorkerTarget(workerGroup: 'other'),
            'deployment-1',
        ), $cancellation);
        $this->gateway->append($this->command(
            'command-other',
            new WorkerTarget(workerGroup: 'other'),
            'deployment-2',
        ), $cancellation);
        $this->gateway->append($this->command(
            'command-expired',
            new WorkerTarget(workerGroup: 'imports'),
            'deployment-expired',
            $this->now->modify('-1 second'),
        ), $cancellation);

        $batch = $this->gateway->receive($identity, new WorkerControlCursor(), $cancellation);

        self::assertFalse($receipt->duplicate);
        self::assertTrue($duplicate->duplicate);
        self::assertSame('command-1', $duplicate->commandId);
        self::assertSame('command-1', $this->gateway->findById('command-1', $cancellation)?->commandId);
        self::assertSame(
            'command-1',
            $this->gateway->findByIdempotencyKey('deployment-1', $cancellation)?->commandId,
        );
        self::assertSame(['command-1'], array_map(
            static fn (WorkerControlCommand $command): string => $command->commandId,
            $batch->commands,
        ));
        self::assertSame('command-other', $batch->cursor->lastCommandId);

        $this->gateway->append($this->command(
            'command-2',
            new WorkerTarget(workerInstanceId: 'import-worker-1'),
            'deployment-3',
        ), $cancellation);
        $pending = $this->gateway->pendingFor($identity, $batch->cursor, $cancellation);

        self::assertSame(['command-2'], array_map(
            static fn (WorkerControlCommand $command): string => $command->commandId,
            $pending,
        ));
    }

    public function testDesiredStateResolutionAndAcknowledgementUseV6Rules(): void
    {
        $cancellation = new NullCancellation();
        $identity = $this->identity();
        $this->gateway->apply(new WorkerDesiredState(
            'pause-imports',
            WorkerDesiredStateType::Paused,
            new WorkerTarget(workerGroup: 'imports'),
            $this->now,
        ), $cancellation);
        $this->gateway->apply(new WorkerDesiredState(
            'resume-instance',
            WorkerDesiredStateType::Resumed,
            new WorkerTarget(workerInstanceId: 'import-worker-1'),
            $this->now->modify('+1 second'),
            override: true,
        ), $cancellation);

        $resolved = $this->gateway->resolveFor($identity, $cancellation);

        self::assertSame('resume-instance', $resolved?->desiredStateId);
        self::assertCount(2, $this->gateway->list(
            new WorkerTarget(workerGroup: 'imports'),
            $cancellation,
        ));

        $command = $this->command(
            'command-ack',
            new WorkerTarget(workerInstanceId: 'import-worker-1'),
            'deployment-ack',
        );
        $this->gateway->append($command, $cancellation);
        $this->gateway->registerWorker(new WorkerInstance(
            $identity,
            WorkerLifecycleState::Running,
            WorkerActivityState::Idle,
            $this->now,
        ), $cancellation);
        $this->gateway->registerChild(new WorkerChildInstance(
            'child-1',
            $identity->workerInstanceId,
            456,
            WorkerChildState::Running,
            $this->now,
            $this->now,
            'queue-1',
            'message-1',
            'correlation-1',
            'gar.import',
        ), $cancellation);
        $this->gateway->acknowledge(new WorkerControlAcknowledgement(
            'command-ack',
            'import-worker-1',
            WorkerControlAcknowledgementState::Applied,
            $this->now->modify('+2 seconds'),
            'applied',
        ), $cancellation);
        $this->gateway->acknowledge(new WorkerControlAcknowledgement(
            'command-ack',
            'import-worker-1',
            WorkerControlAcknowledgementState::Skipped,
            $this->now->modify('+3 seconds'),
            'already stopping',
        ), $cancellation);
        $this->gateway->heartbeatWorker(
            $identity->workerInstanceId,
            WorkerLifecycleState::Stopping,
            WorkerActivityState::Busy,
            $this->now->modify('+4 seconds'),
            $cancellation,
            childrenCount: 1,
            lastCommandId: 'command-ack',
        );
        $this->gateway->heartbeatChild(
            'child-1',
            WorkerChildState::Running,
            $this->now->modify('+4 seconds'),
            $cancellation,
        );
        $this->gateway->finishChild(
            'child-1',
            WorkerChildState::Succeeded,
            $this->now->modify('+5 seconds'),
            $cancellation,
        );
        $this->gateway->stopWorker(
            $identity->workerInstanceId,
            WorkerLifecycleState::Stopped,
            $this->now->modify('+6 seconds'),
            $cancellation,
        );

        $worker = $this->gateway->getWorker($identity->workerInstanceId, $cancellation);
        self::assertNotNull($worker);
        self::assertSame(WorkerLifecycleState::Stopped, $worker->state);
        self::assertSame('command-ack', $worker->lastCommandId);
        self::assertCount(1, $this->gateway->listWorkers(
            new WorkerTarget(workerGroup: 'imports'),
            $cancellation,
        ));
        $children = $this->gateway->listChildren($identity->workerInstanceId, $cancellation);
        self::assertCount(1, $children);
        self::assertSame(WorkerChildState::Succeeded, $children[0]->state);
        $acknowledgements = $this->gateway->acknowledgementsForCommand('command-ack', $cancellation);
        self::assertCount(1, $acknowledgements);
        self::assertSame(WorkerControlAcknowledgementState::Skipped, $acknowledgements[0]->state);
        self::assertSame('already stopping', $acknowledgements[0]->message);
    }

    private function command(
        string $id,
        WorkerTarget $target,
        string $idempotencyKey,
        ?DateTimeImmutable $expiresAt = null,
    ): WorkerControlCommand {
        return new WorkerControlCommand(
            $id,
            WorkerControlCommandType::Stop,
            $target,
            $this->now,
            source: 'integration-test',
            expiresAt: $expiresAt ?? $this->now->modify('+5 minutes'),
            idempotencyKey: $idempotencyKey,
        );
    }

    private function identity(): WorkerIdentity
    {
        return new WorkerIdentity(
            'import-worker',
            'import-worker-1',
            'imports',
            'worker-host',
            123,
            $this->now,
            WorkerMode::Single,
            'postgres',
            'messages',
            ['gar'],
            ['gar.import'],
            ['gar.*'],
            'worker-id-1',
        );
    }

    private function applySchema(string $schema): void
    {
        foreach (explode(";\n", $schema) as $statement) {
            $statement = trim($statement);
            if ($statement !== '') {
                $this->postgres->query($statement);
            }
        }
    }

    private function dropTables(): void
    {
        foreach ([
            $this->tables->commandAuditTable,
            $this->tables->commandDeliveriesTable,
            $this->tables->acknowledgementsTable,
            $this->tables->childInstancesTable,
            $this->tables->workerInstancesTable,
            $this->tables->desiredStatesTable,
            $this->tables->commandsTable,
        ] as $table) {
            $this->postgres->query('DROP TABLE IF EXISTS "' . $table . '"');
        }
    }
}

final class WorkerControlFrozenClock implements ClockInterface
{
    public function __construct(private readonly DateTimeImmutable $now)
    {
    }

    #[\Override]
    public function now(): DateTimeImmutable
    {
        return $this->now;
    }
}
