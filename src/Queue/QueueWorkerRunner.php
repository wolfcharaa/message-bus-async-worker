<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\NullCancellation;
use Fedor\MessageBus\Exception\NonRetryableMessageExceptionInterface;
use Fedor\MessageBus\Service\ServiceReference;
use Fedor\MessageBus\Service\ServiceScopeFailure;
use Fedor\MessageBus\Service\ServiceScopeFactoryInterface;
use Fedor\MessageBus\Service\ServiceScopeInterface;
use Fedor\MessageBus\Service\ServiceScopeRunner;
use Fedor\MessageBus\Worker\Clock\UtcSystemClock;
use Fedor\MessageBus\Worker\Control\WorkerActivityState;
use Fedor\MessageBus\Worker\Control\WorkerControlRuntime;
use Fedor\MessageBus\Worker\Control\WorkerControlSession;
use Fedor\MessageBus\Worker\Control\WorkerIdentity;
use Fedor\MessageBus\Worker\Control\WorkerMode;
use Psr\Clock\ClockInterface;
use Throwable;

use function Amp\async;
use function Amp\delay;
use function Amp\now;

final readonly class QueueWorkerRunner
{
    private ServiceScopeRunner $scopeRunner;

    public function __construct(
        private MessageConsumerInterface $consumer,
        private QueueLeaseControlInterface $leaseControl,
        ServiceScopeFactoryInterface $scopeFactory,
        private ?WorkerControlRuntime $workerControlRuntime = null,
        private ClockInterface $clock = new UtcSystemClock(),
    ) {
        $this->scopeRunner = new ServiceScopeRunner($scopeFactory);
    }

    public function run(
        ConsumerOptions $consumerOptions,
        QueueWorkerRunnerOptions $runnerOptions = new QueueWorkerRunnerOptions(),
        Cancellation $cancellation = new NullCancellation(),
    ): QueueWorkerRunResult {
        $startedAt = now();
        $lastMessageAt = $startedAt;
        $handled = 0;
        $succeeded = 0;
        $retried = 0;
        $rejected = 0;
        $cancelled = 0;
        $leaseLost = 0;
        $control = $this->controlSession($consumerOptions, $runnerOptions);
        $controlStarted = false;
        $lastControlPollAt = 0.0;
        $lastWorkerHeartbeatAt = 0.0;
        $lastStaleRecoveryAt = null;
        $recycleRequested = false;
        $runnerFailure = null;

        try {
            if ($control !== null) {
                $control->start(new NullCancellation());
                $controlStarted = true;
            }

            while (!$this->shouldStop($startedAt, $lastMessageAt, $handled, $runnerOptions, $cancellation)) {
                if ($control !== null) {
                    if ($control->shouldStop()) {
                        break;
                    }
                    try {
                        $this->maintainControl(
                            $control,
                            $runnerOptions,
                            WorkerActivityState::Idle,
                            $cancellation,
                            $lastControlPollAt,
                            $lastWorkerHeartbeatAt,
                            force: $lastControlPollAt === 0.0,
                        );
                    } catch (CancelledException) {
                        break;
                    }
                    if ($control->shouldStop()) {
                        break;
                    }
                    if ($control->isPaused()) {
                        if (!$this->idle($runnerOptions, $cancellation)) {
                            break;
                        }
                        continue;
                    }
                }

                try {
                    $lastStaleRecoveryAt = $this->maintainQueue(
                        $consumerOptions,
                        $runnerOptions,
                        $cancellation,
                        $lastStaleRecoveryAt,
                    );
                } catch (CancelledException) {
                    break;
                }

                try {
                    $message = $this->consumer->next($consumerOptions, $cancellation);
                } catch (CancelledException) {
                    break;
                }

                if ($message === null) {
                    if ($runnerOptions->stopWhenEmpty) {
                        break;
                    }
                    if (!$this->idle($runnerOptions, $cancellation)) {
                        break;
                    }
                    continue;
                }

                $lastMessageAt = now();
                ++$handled;

                try {
                    $this->handle($message, $runnerOptions, $cancellation, $control);
                    $this->consumer->ack($message, new NullCancellation());
                    ++$succeeded;
                } catch (QueueLeaseLost) {
                    ++$leaseLost;
                } catch (Throwable $error) {
                    if ($error instanceof ServiceScopeFailure) {
                        $this->consumer->reject(
                            $message,
                            QueueOutcomeUnknown::forMessage($message, $error),
                            new NullCancellation(),
                        );
                        ++$rejected;
                        $recycleRequested = true;
                        break;
                    }

                    $reason = $this->cancellationReason($error);
                    if ($reason instanceof QueueCancellationRequested) {
                        $this->consumer->cancel($message, $reason, new NullCancellation());
                        ++$cancelled;
                        continue;
                    }

                    $failure = $error instanceof CancelledException
                        ? QueueOutcomeUnknown::forMessage($message, $reason ?? $error)
                        : ($reason ?? $error);
                    if ($failure instanceof NonRetryableMessageExceptionInterface || !$message->canRetry()) {
                        $this->consumer->reject($message, $failure, new NullCancellation());
                        ++$rejected;
                        continue;
                    }

                    $this->consumer->retry($message, $failure, new NullCancellation());
                    ++$retried;
                }

                if ($control !== null) {
                    $control->heartbeat(WorkerActivityState::Idle, new NullCancellation());
                }
            }
        } catch (Throwable $error) {
            $runnerFailure = $error;
            throw $error;
        } finally {
            if ($control !== null && $controlStarted) {
                try {
                    $control->stop(new NullCancellation(), $runnerFailure);
                } catch (Throwable $stopError) {
                    if ($runnerFailure === null) {
                        throw $stopError;
                    }
                }
            }
        }

        return new QueueWorkerRunResult(
            $handled,
            $succeeded,
            $retried,
            $rejected,
            $cancelled,
            $leaseLost,
            $recycleRequested || $control?->restartRequested() === true
                ? $runnerOptions->restartExitCode
                : null,
        );
    }

    private function handle(
        ReceivedQueueMessage $message,
        QueueWorkerRunnerOptions $options,
        Cancellation $processCancellation,
        ?WorkerControlSession $control,
    ): void {
        $jobCancellation = new DeferredCancellation();
        $monitorStop = new DeferredCancellation();
        $jobStartedAt = now();
        $monitorError = null;
        $monitor = async(function () use (
            $message,
            $options,
            $processCancellation,
            $jobCancellation,
            $monitorStop,
            $control,
            $jobStartedAt,
            &$monitorError,
        ): void {
            $lastHeartbeatAt = now();
            $lastCancellationPollAt = $lastHeartbeatAt;
            $lastControlPollAt = $lastHeartbeatAt;
            $lastWorkerHeartbeatAt = $lastHeartbeatAt;
            $shutdownRequestedAt = null;
            $tickMilliseconds = min(
                $options->heartbeatIntervalMilliseconds,
                $options->cancellationPollIntervalMilliseconds,
                $control === null ? 100 : $options->controlPollIntervalMilliseconds,
                100,
            );

            try {
                while (!$monitorStop->isCancelled()) {
                    delay($tickMilliseconds / 1000, reference: false, cancellation: $monitorStop->getCancellation());
                    $current = now();

                    if (
                        !$jobCancellation->isCancelled()
                        && $current - $jobStartedAt >= $options->maxJobRuntimeSeconds
                    ) {
                        $jobCancellation->cancel(JobRuntimeExpired::forMessage(
                            $message,
                            $options->maxJobRuntimeSeconds,
                        ));
                    }

                    if ($processCancellation->isRequested()) {
                        $shutdownRequestedAt ??= $current;
                        if (
                            !$jobCancellation->isCancelled()
                            && $current - $shutdownRequestedAt >= $options->gracefulShutdownTimeoutSeconds
                        ) {
                            $jobCancellation->cancel(GracefulShutdownExpired::forMessage($message));
                        }
                    }

                    if (($current - $lastHeartbeatAt) * 1000 >= $options->heartbeatIntervalMilliseconds) {
                        $this->leaseControl->heartbeat($message, $monitorStop->getCancellation());
                        $lastHeartbeatAt = $current;
                    }

                    if (($current - $lastCancellationPollAt) * 1000 >= $options->cancellationPollIntervalMilliseconds) {
                        if ($this->leaseControl->isCancellationRequested(
                            $message,
                            $monitorStop->getCancellation(),
                        ) && !$jobCancellation->isCancelled()) {
                            $jobCancellation->cancel(QueueCancellationRequested::forMessage($message));
                        }
                        $lastCancellationPollAt = $current;
                    }

                    if (
                        $control !== null
                        && ($current - $lastControlPollAt) * 1000 >= $options->controlPollIntervalMilliseconds
                    ) {
                        $control->poll($monitorStop->getCancellation());
                        if ($control->shouldKillCurrentJob() && !$jobCancellation->isCancelled()) {
                            $jobCancellation->cancel(WorkerControlKillRequested::forMessage($message));
                        }
                        $lastControlPollAt = $current;
                    }

                    if (
                        $control !== null
                        && ($current - $lastWorkerHeartbeatAt) * 1000 >= $options->heartbeatIntervalMilliseconds
                    ) {
                        $control->heartbeat(
                            WorkerActivityState::Busy,
                            $monitorStop->getCancellation(),
                        );
                        $lastWorkerHeartbeatAt = $current;
                    }
                }
            } catch (CancelledException $error) {
                if (!$monitorStop->isCancelled()) {
                    $monitorError = $error;
                    if (!$jobCancellation->isCancelled()) {
                        $jobCancellation->cancel($error);
                    }
                }
            } catch (Throwable $error) {
                $monitorError = $error;
                if (!$jobCancellation->isCancelled()) {
                    $jobCancellation->cancel($error);
                }
            }
        });

        $handlerError = null;
        try {
            $this->scopeRunner->run(function (ServiceScopeInterface $scope) use ($message, $jobCancellation): void {
                $worker = $scope->resolve(ServiceReference::forClass(QueueWorkerInterface::class));
                $worker->handle($message, $jobCancellation->getCancellation());
            });
        } catch (Throwable $error) {
            $handlerError = $error;
        } finally {
            $monitorStop->cancel();
            try {
                $monitor->await();
            } catch (CancelledException) {
            }
        }

        if ($monitorError !== null) {
            if (!$monitorError instanceof QueueLeaseLost) {
                throw QueueOutcomeUnknown::forMessage($message, $monitorError);
            }

            throw $monitorError;
        }
        if ($handlerError !== null) {
            throw $handlerError;
        }
    }

    private function maintainQueue(
        ConsumerOptions $consumerOptions,
        QueueWorkerRunnerOptions $runnerOptions,
        Cancellation $cancellation,
        ?float $lastStaleRecoveryAt,
    ): float {
        $current = now();
        if (
            $lastStaleRecoveryAt !== null
            && $current - $lastStaleRecoveryAt < $runnerOptions->staleRecoveryIntervalSeconds
        ) {
            return $lastStaleRecoveryAt;
        }

        $this->leaseControl->recoverStale($consumerOptions, $cancellation);

        return now();
    }

    private function shouldStop(
        float $startedAt,
        float $lastMessageAt,
        int $handled,
        QueueWorkerRunnerOptions $options,
        Cancellation $cancellation,
    ): bool {
        if ($cancellation->isRequested()) {
            return true;
        }
        if ($options->maxMessages !== null && $handled >= $options->maxMessages) {
            return true;
        }
        if ($options->maxRuntimeSeconds !== null && now() - $startedAt >= $options->maxRuntimeSeconds) {
            return true;
        }
        if ($options->idleTimeoutSeconds !== null && now() - $lastMessageAt >= $options->idleTimeoutSeconds) {
            return true;
        }

        return $options->memoryLimitBytes !== null && memory_get_usage(true) >= $options->memoryLimitBytes;
    }

    private function cancellationReason(Throwable $error): ?Throwable
    {
        if (!$error instanceof CancelledException) {
            return null;
        }

        return $error->getPrevious();
    }

    private function idle(QueueWorkerRunnerOptions $options, Cancellation $cancellation): bool
    {
        try {
            delay($options->sleepWhenIdleMilliseconds / 1000, cancellation: $cancellation);

            return true;
        } catch (CancelledException) {
            return false;
        }
    }

    private function maintainControl(
        WorkerControlSession $control,
        QueueWorkerRunnerOptions $options,
        WorkerActivityState $activity,
        Cancellation $cancellation,
        float &$lastControlPollAt,
        float &$lastWorkerHeartbeatAt,
        bool $force = false,
    ): void {
        $current = now();
        if ($force || ($current - $lastControlPollAt) * 1000 >= $options->controlPollIntervalMilliseconds) {
            $control->poll($cancellation);
            $lastControlPollAt = $current;
        }
        if ($force || ($current - $lastWorkerHeartbeatAt) * 1000 >= $options->heartbeatIntervalMilliseconds) {
            $control->heartbeat($activity, $cancellation);
            $lastWorkerHeartbeatAt = $current;
        }
    }

    private function controlSession(
        ConsumerOptions $consumerOptions,
        QueueWorkerRunnerOptions $runnerOptions,
    ): ?WorkerControlSession {
        if ($this->workerControlRuntime === null) {
            return null;
        }

        return new WorkerControlSession(
            $this->workerControlRuntime,
            $this->identity($consumerOptions, $runnerOptions),
            $this->clock,
        );
    }

    private function identity(
        ConsumerOptions $consumerOptions,
        QueueWorkerRunnerOptions $runnerOptions,
    ): WorkerIdentity {
        $host = $runnerOptions->host ?? (gethostname() ?: 'unknown');
        $pid = getmypid() ?: 0;
        $workerName = $runnerOptions->workerName ?? $consumerOptions->workerId;

        return new WorkerIdentity(
            workerName: $workerName,
            workerInstanceId: $runnerOptions->workerInstanceId
                ?? $this->workerInstanceId($workerName, $host, $pid),
            workerGroup: $runnerOptions->workerGroup,
            host: $host,
            pid: $pid,
            startedAt: $this->clock->now(),
            mode: WorkerMode::Single,
            transport: $consumerOptions->transport,
            queue: $consumerOptions->queue,
            flows: $consumerOptions->flows,
            bindingIds: $consumerOptions->bindingIds,
            bindingPatterns: $consumerOptions->bindingPatterns,
            workerId: $consumerOptions->workerId,
        );
    }

    private function workerInstanceId(string $workerName, string $host, int $pid): string
    {
        return sprintf(
            'worker.%s.%s.%d.%s',
            $workerName,
            $host,
            $pid,
            bin2hex(random_bytes(6)),
        );
    }
}
