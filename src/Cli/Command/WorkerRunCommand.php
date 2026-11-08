<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Cli\Command;

use Fedor\MessageBus\Worker\Cli\BootstrapResolver;
use Fedor\MessageBus\Worker\Cli\InputOptionValue;
use Fedor\MessageBus\Worker\Process\PcntlSignalCancellation;
use Fedor\MessageBus\Worker\Queue\ConsumerOptions;
use Fedor\MessageBus\Worker\Queue\QueueWorkerRunnerOptions;
use Fedor\MessageBus\Worker\Runtime\WorkerRuntime;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

final class WorkerRunCommand extends Command
{
    public function __construct(
        private readonly BootstrapResolver $bootstrapResolver = new BootstrapResolver(),
    ) {
        parent::__construct('worker:run');
    }

    protected function configure(): void
    {
        $this
            ->setDescription('Run the AMPHP queue worker.')
            ->addOption('bootstrap', null, InputOption::VALUE_REQUIRED)
            ->addOption('transport', null, InputOption::VALUE_REQUIRED, default: 'postgres')
            ->addOption('queue', null, InputOption::VALUE_REQUIRED, default: 'default')
            ->addOption('worker-id', null, InputOption::VALUE_REQUIRED, default: 'message-bus-worker')
            ->addOption('worker-name', null, InputOption::VALUE_REQUIRED)
            ->addOption('worker-group', null, InputOption::VALUE_REQUIRED)
            ->addOption('worker-instance-id', null, InputOption::VALUE_REQUIRED)
            ->addOption('host', null, InputOption::VALUE_REQUIRED)
            ->addOption('flow', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY)
            ->addOption('binding-id', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY)
            ->addOption('binding-pattern', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY)
            ->addOption('consumer-timeout', null, InputOption::VALUE_REQUIRED, default: 5)
            ->addOption('consumer-limit', null, InputOption::VALUE_REQUIRED, default: 1)
            ->addOption('lock-ttl', null, InputOption::VALUE_REQUIRED, default: 300)
            ->addOption('stale-recovery-batch', null, InputOption::VALUE_REQUIRED, default: 100)
            ->addOption('stale-recovery-interval', null, InputOption::VALUE_REQUIRED, default: 30)
            ->addOption('max-messages', null, InputOption::VALUE_REQUIRED)
            ->addOption('max-runtime', null, InputOption::VALUE_REQUIRED)
            ->addOption('max-job-runtime', null, InputOption::VALUE_REQUIRED, default: 300)
            ->addOption('idle-timeout', null, InputOption::VALUE_REQUIRED)
            ->addOption('sleep-when-idle', null, InputOption::VALUE_REQUIRED, default: 500)
            ->addOption('memory-limit', null, InputOption::VALUE_REQUIRED)
            ->addOption('heartbeat-interval', null, InputOption::VALUE_REQUIRED, default: 10000)
            ->addOption('cancellation-poll-interval', null, InputOption::VALUE_REQUIRED, default: 500)
            ->addOption('graceful-shutdown-timeout', null, InputOption::VALUE_REQUIRED, default: 30)
            ->addOption('control-poll-interval', null, InputOption::VALUE_REQUIRED, default: 1000)
            ->addOption('restart-exit-code', null, InputOption::VALUE_REQUIRED, default: 75)
            ->addOption('stop-when-empty', null, InputOption::VALUE_NONE);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $consumerOptions = new ConsumerOptions(
            transport: InputOptionValue::string($input, 'transport'),
            queue: InputOptionValue::string($input, 'queue'),
            timeoutSeconds: InputOptionValue::integer($input, 'consumer-timeout'),
            limit: InputOptionValue::integer($input, 'consumer-limit'),
            workerId: InputOptionValue::string($input, 'worker-id'),
            lockTtlSeconds: InputOptionValue::integer($input, 'lock-ttl'),
            flows: InputOptionValue::stringList($input, 'flow'),
            bindingIds: InputOptionValue::stringList($input, 'binding-id'),
            bindingPatterns: InputOptionValue::stringList($input, 'binding-pattern'),
            staleRecoveryBatchSize: InputOptionValue::integer($input, 'stale-recovery-batch'),
        );
        $runnerOptions = new QueueWorkerRunnerOptions(
            maxMessages: InputOptionValue::nullableInteger($input, 'max-messages'),
            maxRuntimeSeconds: InputOptionValue::nullableInteger($input, 'max-runtime'),
            idleTimeoutSeconds: InputOptionValue::nullableInteger($input, 'idle-timeout'),
            stopWhenEmpty: InputOptionValue::boolean($input, 'stop-when-empty'),
            sleepWhenIdleMilliseconds: InputOptionValue::integer($input, 'sleep-when-idle'),
            memoryLimitBytes: InputOptionValue::nullableInteger($input, 'memory-limit'),
            heartbeatIntervalMilliseconds: InputOptionValue::integer($input, 'heartbeat-interval'),
            cancellationPollIntervalMilliseconds: InputOptionValue::integer($input, 'cancellation-poll-interval'),
            gracefulShutdownTimeoutSeconds: InputOptionValue::integer($input, 'graceful-shutdown-timeout'),
            maxJobRuntimeSeconds: InputOptionValue::integer($input, 'max-job-runtime'),
            staleRecoveryIntervalSeconds: InputOptionValue::integer($input, 'stale-recovery-interval'),
            workerName: InputOptionValue::nullableString($input, 'worker-name'),
            workerGroup: InputOptionValue::nullableString($input, 'worker-group'),
            workerInstanceId: InputOptionValue::nullableString($input, 'worker-instance-id'),
            host: InputOptionValue::nullableString($input, 'host'),
            controlPollIntervalMilliseconds: InputOptionValue::integer($input, 'control-poll-interval'),
            restartExitCode: InputOptionValue::integer($input, 'restart-exit-code'),
        );
        $bootstrapPath = InputOptionValue::nullableString($input, 'bootstrap');

        return $this->bootstrapResolver->run(
            $bootstrapPath,
            static function (WorkerRuntime $runtime) use ($consumerOptions, $runnerOptions, $output): int {
                $signalCancellation = new PcntlSignalCancellation();

                try {
                    $result = $runtime->runner->run(
                        $consumerOptions,
                        $runnerOptions,
                        $signalCancellation->cancellation(),
                    );
                } finally {
                    $signalCancellation->close();
                }

                $output->writeln(sprintf(
                    'handled=%d succeeded=%d retried=%d rejected=%d cancelled=%d lease_lost=%d',
                    $result->handled,
                    $result->succeeded,
                    $result->retried,
                    $result->rejected,
                    $result->cancelled,
                    $result->leaseLost,
                ));

                return $result->exitCode ?? self::SUCCESS;
            },
        );
    }
}
