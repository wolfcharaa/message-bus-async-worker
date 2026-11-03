<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Tests;

use Amp\Cancellation;
use DateTimeImmutable;
use Fedor\MessageBus\Service\ServiceReference;
use Fedor\MessageBus\Service\ServiceScopeFactoryInterface;
use Fedor\MessageBus\Service\ServiceScopeInterface;
use Fedor\MessageBus\Worker\Cli\ApplicationFactory;
use Fedor\MessageBus\Worker\Cli\BootstrapResolver;
use Fedor\MessageBus\Worker\Cli\Command\WorkerControlActionCommand;
use Fedor\MessageBus\Worker\Cli\Command\WorkerPostgresSchemaCommand;
use Fedor\MessageBus\Worker\Cli\Command\WorkerRunCommand;
use Fedor\MessageBus\Worker\Cli\Command\WorkerStatusCommand;
use Fedor\MessageBus\Worker\Control\WorkerActivityState;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandReceipt;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandRepositoryInterface;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandType;
use Fedor\MessageBus\Worker\Control\WorkerControlInboxInterface;
use Fedor\MessageBus\Worker\Control\WorkerControlRequest;
use Fedor\MessageBus\Worker\Control\WorkerControlRuntime;
use Fedor\MessageBus\Worker\Control\WorkerControlServiceInterface;
use Fedor\MessageBus\Worker\Control\WorkerDesiredStateRepositoryInterface;
use Fedor\MessageBus\Worker\Control\WorkerIdentity;
use Fedor\MessageBus\Worker\Control\WorkerInstance;
use Fedor\MessageBus\Worker\Control\WorkerLifecycleState;
use Fedor\MessageBus\Worker\Control\WorkerMode;
use Fedor\MessageBus\Worker\Control\WorkerRegistryInterface;
use Fedor\MessageBus\Worker\Control\WorkerStatusRepositoryInterface;
use Fedor\MessageBus\Worker\Control\WorkerStatusServiceInterface;
use Fedor\MessageBus\Worker\Control\WorkerTarget;
use Fedor\MessageBus\Worker\Queue\ConsumerOptions;
use Fedor\MessageBus\Worker\Queue\MessageConsumerInterface;
use Fedor\MessageBus\Worker\Queue\QueueLeaseControlInterface;
use Fedor\MessageBus\Worker\Queue\QueueWorkerRunner;
use Fedor\MessageBus\Worker\Queue\ReceivedQueueMessage;
use Fedor\MessageBus\Worker\Runtime\WorkerRuntime;
use LogicException;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\Console\Tester\CommandTester;
use Throwable;

final class WorkerCliTest extends TestCase
{
    private static ?WorkerRuntime $bootstrapRuntime = null;

    protected function tearDown(): void
    {
        self::$bootstrapRuntime = null;
    }

    public function testApplicationExposesTheV6WorkerCommandSurface(): void
    {
        $application = ApplicationFactory::create();

        foreach ([
            'worker:run',
            'worker:control',
            'worker:pause',
            'worker:resume',
            'worker:drain',
            'worker:stop',
            'worker:kill',
            'worker:restart',
            'worker:status',
            'worker:schema:postgres',
        ] as $command) {
            self::assertTrue($application->has($command), $command);
        }
    }

    public function testRunCommandLoadsExplicitRuntimeAndStopsOnEmptyQueue(): void
    {
        self::$bootstrapRuntime = self::runtime();
        $tester = new CommandTester(new WorkerRunCommand(new BootstrapResolver()));

        $exitCode = $tester->execute([
            '--bootstrap' => self::bootstrapPath(),
            '--transport' => 'postgres',
            '--queue' => 'messages',
            '--stop-when-empty' => true,
        ]);

        self::assertSame(0, $exitCode);
        self::assertStringContainsString(
            'handled=0 succeeded=0 retried=0 rejected=0 cancelled=0 lease_lost=0',
            $tester->getDisplay(),
        );
    }

    public function testControlAliasBuildsTargetAndAuditRequest(): void
    {
        $target = new WorkerTarget(workerId: 'worker-1');
        $receipt = new WorkerControlCommandReceipt(
            'command-1',
            WorkerControlCommandType::Pause,
            $target,
            new DateTimeImmutable('2026-11-03T12:00:00+00:00'),
        );
        $controlService = $this->createMock(WorkerControlServiceInterface::class);
        $controlService->expects(self::once())
            ->method('pause')
            ->with(
                self::callback(static fn (WorkerTarget $actual): bool => $actual->workerId === 'worker-1'),
                self::callback(static fn (WorkerControlRequest $request): bool => $request->source === 'cli'
                    && $request->reason === 'maintenance'),
            )
            ->willReturn($receipt);
        self::$bootstrapRuntime = self::runtime($this->controlRuntime($controlService));
        $tester = new CommandTester(new WorkerControlActionCommand(
            WorkerControlCommandType::Pause,
            'worker:pause',
            new BootstrapResolver(),
        ));

        $exitCode = $tester->execute([
            '--bootstrap' => self::bootstrapPath(),
            '--worker-id' => 'worker-1',
            '--reason' => 'maintenance',
        ]);

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('command=command-1 type=pause duplicate=no', $tester->getDisplay());
    }

    public function testStatusDefaultsToAllWorkersAndRendersRuntimeState(): void
    {
        $worker = new WorkerInstance(
            new WorkerIdentity(
                workerName: 'importer',
                workerInstanceId: 'worker-instance-1',
                workerGroup: 'imports',
                host: 'worker-host',
                pid: 42,
                startedAt: new DateTimeImmutable('2026-11-03T12:00:00+00:00'),
                mode: WorkerMode::Single,
                transport: 'postgres',
                queue: 'addresses',
            ),
            WorkerLifecycleState::Running,
            WorkerActivityState::Busy,
            new DateTimeImmutable('2026-11-03T12:01:00+00:00'),
        );
        $statusService = $this->createMock(WorkerStatusServiceInterface::class);
        $statusService->expects(self::once())
            ->method('listWorkers')
            ->with(self::callback(static fn (WorkerTarget $target): bool => $target->all))
            ->willReturn([$worker]);
        self::$bootstrapRuntime = self::runtime($this->controlRuntime(
            $this->createStub(WorkerControlServiceInterface::class),
            $statusService,
        ));
        $tester = new CommandTester(new WorkerStatusCommand(new BootstrapResolver()));

        $exitCode = $tester->execute(['--bootstrap' => self::bootstrapPath()]);

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('worker-instance-1', $tester->getDisplay());
        self::assertStringContainsString('postgres/addresses', $tester->getDisplay());
    }

    public function testPostgresSchemaCommandPrintsWorkerControlDdl(): void
    {
        $tester = new CommandTester(new WorkerPostgresSchemaCommand());

        self::assertSame(0, $tester->execute([]));
        self::assertStringContainsString(
            'CREATE TABLE IF NOT EXISTS "message_bus__worker_control_commands"',
            $tester->getDisplay(),
        );
    }

    public function testBootstrapRejectsAnInvalidCompositionRoot(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Worker bootstrap must return');

        (new BootstrapResolver())->resolve(__DIR__ . '/Fixture/invalid-worker-bootstrap.php');
    }

    public static function bootstrapRuntime(): WorkerRuntime
    {
        return self::$bootstrapRuntime ?? throw new LogicException('CLI test bootstrap runtime is not prepared.');
    }

    private static function bootstrapPath(): string
    {
        return __DIR__ . '/Fixture/worker-bootstrap.php';
    }

    private static function runtime(?WorkerControlRuntime $control = null): WorkerRuntime
    {
        return new WorkerRuntime(
            new QueueWorkerRunner(
                new CliEmptyConsumer(),
                new CliLeaseControl(),
                new CliScopeFactory(),
                $control,
            ),
            $control,
        );
    }

    private function controlRuntime(
        WorkerControlServiceInterface $controlService,
        ?WorkerStatusServiceInterface $statusService = null,
    ): WorkerControlRuntime {
        return new WorkerControlRuntime(
            $controlService,
            $this->createStub(WorkerControlCommandRepositoryInterface::class),
            $this->createStub(WorkerDesiredStateRepositoryInterface::class),
            $this->createStub(WorkerRegistryInterface::class),
            $this->createStub(WorkerStatusRepositoryInterface::class),
            $this->createStub(WorkerControlInboxInterface::class),
            $statusService,
        );
    }
}

final class CliEmptyConsumer implements MessageConsumerInterface
{
    public function next(ConsumerOptions $options, Cancellation $cancellation): ?ReceivedQueueMessage
    {
        return null;
    }

    public function ack(ReceivedQueueMessage $message, Cancellation $cancellation): void
    {
    }

    public function retry(ReceivedQueueMessage $message, Throwable $reason, Cancellation $cancellation): void
    {
    }

    public function reject(ReceivedQueueMessage $message, Throwable $reason, Cancellation $cancellation): void
    {
    }

    public function cancel(ReceivedQueueMessage $message, Throwable $reason, Cancellation $cancellation): void
    {
    }
}

final class CliLeaseControl implements QueueLeaseControlInterface
{
    public function heartbeat(ReceivedQueueMessage $message, Cancellation $cancellation): void
    {
    }

    public function isCancellationRequested(ReceivedQueueMessage $message, Cancellation $cancellation): bool
    {
        return false;
    }

    public function recoverStale(ConsumerOptions $options, Cancellation $cancellation): int
    {
        return 0;
    }
}

final class CliScopeFactory implements ServiceScopeFactoryInterface
{
    public function openScope(): ServiceScopeInterface
    {
        return new CliScope();
    }
}

final class CliScope implements ServiceScopeInterface
{
    public function resolve(ServiceReference $reference): object
    {
        throw new LogicException('The empty CLI test consumer must not open a service scope.');
    }

    public function dispose(?Throwable $error = null): void
    {
    }
}
