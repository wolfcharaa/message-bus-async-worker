<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use Amp\Cancellation;
use Fedor\MessageBus\Worker\Clock\UtcSystemClock;
use Psr\Clock\ClockInterface;

final class WorkerControlSession
{
    private WorkerControlCursor $cursor;
    private WorkerLifecycleState $lifecycle = WorkerLifecycleState::Starting;
    private bool $paused = false;
    private bool $draining = false;
    private bool $stopping = false;
    private bool $killing = false;
    private bool $restarting = false;
    private ?string $lastCommandId = null;

    public function __construct(
        private readonly WorkerControlRuntime $runtime,
        public readonly WorkerIdentity $identity,
        private readonly ClockInterface $clock = new UtcSystemClock(),
    ) {
        $this->cursor = new WorkerControlCursor();
    }

    public function start(Cancellation $cancellation): void
    {
        $this->runtime->workerRegistry->registerWorker(new WorkerInstance(
            $this->identity,
            WorkerLifecycleState::Starting,
            WorkerActivityState::Idle,
            $this->clock->now(),
        ), $cancellation);
        $this->lifecycle = WorkerLifecycleState::Running;
    }

    public function poll(Cancellation $cancellation): void
    {
        $batch = $this->runtime->inbox->receive($this->identity, $this->cursor, $cancellation);
        $this->cursor = $batch->cursor;

        foreach ($batch->commands as $command) {
            $this->apply($command);
            $this->runtime->commandRepository->acknowledge(new WorkerControlAcknowledgement(
                $command->commandId,
                $this->identity->workerInstanceId,
                WorkerControlAcknowledgementState::Applied,
                $this->clock->now(),
            ), $cancellation);
        }

        if (!$this->hasTerminalRequest()) {
            if ($batch->desiredState?->type === WorkerDesiredStateType::Paused) {
                $this->pause();
            }
            if ($batch->desiredState?->type === WorkerDesiredStateType::Resumed) {
                $this->resume();
            }
        }
    }

    public function heartbeat(WorkerActivityState $activity, Cancellation $cancellation): void
    {
        $this->runtime->workerRegistry->heartbeatWorker(
            $this->identity->workerInstanceId,
            $this->lifecycle,
            $activity,
            $this->clock->now(),
            $cancellation,
            lastCommandId: $this->lastCommandId,
        );
    }

    public function stop(
        Cancellation $cancellation,
        ?\Throwable $failure = null,
    ): void {
        $state = match (true) {
            $failure !== null => WorkerLifecycleState::Failed,
            $this->restarting => WorkerLifecycleState::Restarting,
            default => WorkerLifecycleState::Stopped,
        };
        $this->lifecycle = $state;
        $this->runtime->workerRegistry->stopWorker(
            $this->identity->workerInstanceId,
            $state,
            $this->clock->now(),
            $cancellation,
            $failure?->getMessage(),
        );
    }

    public function isPaused(): bool
    {
        return $this->paused;
    }

    public function shouldStop(): bool
    {
        return $this->hasTerminalRequest();
    }

    public function shouldKillCurrentJob(): bool
    {
        return $this->killing;
    }

    public function restartRequested(): bool
    {
        return $this->restarting;
    }

    public function lifecycle(): WorkerLifecycleState
    {
        return $this->lifecycle;
    }

    public function cursor(): WorkerControlCursor
    {
        return $this->cursor;
    }

    private function apply(WorkerControlCommand $command): void
    {
        $this->lastCommandId = $command->commandId;

        switch ($command->type) {
            case WorkerControlCommandType::Pause:
                $this->pause();
                break;
            case WorkerControlCommandType::Resume:
                $this->resume();
                break;
            case WorkerControlCommandType::Drain:
                $this->terminate(WorkerLifecycleState::Draining);
                break;
            case WorkerControlCommandType::Stop:
                $this->terminate(WorkerLifecycleState::Stopping);
                break;
            case WorkerControlCommandType::Kill:
                $this->terminate(WorkerLifecycleState::Killing);
                break;
            case WorkerControlCommandType::Restart:
                $this->terminate(WorkerLifecycleState::Restarting);
                break;
        }
    }

    private function pause(): void
    {
        $this->paused = true;
        $this->lifecycle = WorkerLifecycleState::Paused;
    }

    private function resume(): void
    {
        $this->paused = false;
        $this->lifecycle = WorkerLifecycleState::Running;
    }

    private function terminate(WorkerLifecycleState $state): void
    {
        match ($state) {
            WorkerLifecycleState::Draining => $this->draining = true,
            WorkerLifecycleState::Stopping => $this->stopping = true,
            WorkerLifecycleState::Killing => $this->killing = true,
            WorkerLifecycleState::Restarting => $this->restarting = true,
            default => null,
        };
        $this->lifecycle = $state;
    }

    private function hasTerminalRequest(): bool
    {
        return $this->draining || $this->stopping || $this->killing || $this->restarting;
    }
}
