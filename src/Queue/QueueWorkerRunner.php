<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\NullCancellation;
use Fedor\MessageBus\Exception\NonRetryableMessageExceptionInterface;
use Fedor\MessageBus\Service\ServiceReference;
use Fedor\MessageBus\Service\ServiceScopeFactoryInterface;
use Fedor\MessageBus\Service\ServiceScopeInterface;
use Fedor\MessageBus\Service\ServiceScopeRunner;
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

        while (!$this->shouldStop($startedAt, $lastMessageAt, $handled, $runnerOptions, $cancellation)) {
            try {
                $message = $this->consumer->next($consumerOptions, $cancellation);
            } catch (CancelledException) {
                break;
            }

            if ($message === null) {
                if ($runnerOptions->stopWhenEmpty) {
                    break;
                }

                try {
                    delay($runnerOptions->sleepWhenIdleMilliseconds / 1000, cancellation: $cancellation);
                } catch (CancelledException) {
                    break;
                }
                continue;
            }

            $lastMessageAt = now();
            ++$handled;

            try {
                $this->handle($message, $runnerOptions, $cancellation);
                $this->consumer->ack($message, new NullCancellation());
                ++$succeeded;
            } catch (QueueLeaseLost) {
                ++$leaseLost;
            } catch (Throwable $error) {
                $reason = $this->cancellationReason($error);
                if ($reason instanceof QueueCancellationRequested) {
                    $this->consumer->cancel($message, $reason, new NullCancellation());
                    ++$cancelled;
                    continue;
                }

                if ($error instanceof NonRetryableMessageExceptionInterface || !$message->canRetry()) {
                    $this->consumer->reject($message, $error, new NullCancellation());
                    ++$rejected;
                    continue;
                }

                $this->consumer->retry($message, $error, new NullCancellation());
                ++$retried;
            }
        }

        return new QueueWorkerRunResult($handled, $succeeded, $retried, $rejected, $cancelled, $leaseLost);
    }

    private function handle(
        ReceivedQueueMessage $message,
        QueueWorkerRunnerOptions $options,
        Cancellation $processCancellation,
    ): void {
        $jobCancellation = new DeferredCancellation();
        $monitorStop = new DeferredCancellation();
        $monitorError = null;
        $monitor = async(function () use (
            $message,
            $options,
            $processCancellation,
            $jobCancellation,
            $monitorStop,
            &$monitorError,
        ): void {
            $lastHeartbeatAt = now();
            $lastCancellationPollAt = $lastHeartbeatAt;
            $shutdownRequestedAt = null;
            $tickMilliseconds = min(
                $options->heartbeatIntervalMilliseconds,
                $options->cancellationPollIntervalMilliseconds,
                100,
            );

            try {
                while (!$monitorStop->isCancelled()) {
                    delay($tickMilliseconds / 1000, reference: false, cancellation: $monitorStop->getCancellation());
                    $current = now();

                    if ($processCancellation->isRequested()) {
                        $shutdownRequestedAt ??= $current;
                        if ($current - $shutdownRequestedAt >= $options->gracefulShutdownTimeoutSeconds) {
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
                        )) {
                            $jobCancellation->cancel(QueueCancellationRequested::forMessage($message));
                        }
                        $lastCancellationPollAt = $current;
                    }
                }
            } catch (CancelledException $error) {
                if (!$monitorStop->isCancelled()) {
                    $monitorError = $error;
                    $jobCancellation->cancel($error);
                }
            } catch (Throwable $error) {
                $monitorError = $error;
                $jobCancellation->cancel($error);
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
            throw $monitorError;
        }
        if ($handlerError !== null) {
            throw $handlerError;
        }
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
}
