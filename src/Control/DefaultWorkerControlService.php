<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use Amp\Cancellation;
use Amp\NullCancellation;
use Fedor\MessageBus\Worker\Clock\UtcSystemClock;
use Psr\Clock\ClockInterface;

final readonly class DefaultWorkerControlService implements WorkerControlServiceInterface
{
    public const DEFAULT_GRACEFUL_COMMAND_TTL_SECONDS = 300;
    public const DEFAULT_KILL_COMMAND_TTL_SECONDS = 60;

    public function __construct(
        private WorkerControlCommandRepositoryInterface $commands,
        private WorkerDesiredStateRepositoryInterface $desiredStates,
        private WorkerControlIdGeneratorInterface $ids = new RandomWorkerControlIdGenerator(),
        private ClockInterface $clock = new UtcSystemClock(),
    ) {
    }

    public function send(
        WorkerControlCommand $command,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt {
        $receipt = $this->commands->append($command, $cancellation);

        if (!$receipt->duplicate && $command->type === WorkerControlCommandType::Pause) {
            $this->desiredStates->apply(
                $this->desiredState($command, WorkerDesiredStateType::Paused),
                $cancellation,
            );
        }
        if (!$receipt->duplicate && $command->type === WorkerControlCommandType::Resume) {
            $this->desiredStates->apply(
                $this->desiredState($command, WorkerDesiredStateType::Resumed),
                $cancellation,
            );
        }

        return $receipt;
    }

    public function pause(
        WorkerTarget $target,
        ?WorkerControlRequest $request = null,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt {
        return $this->send($this->command(WorkerControlCommandType::Pause, $target, $request), $cancellation);
    }

    public function resume(
        WorkerTarget $target,
        ?WorkerControlRequest $request = null,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt {
        return $this->send($this->command(WorkerControlCommandType::Resume, $target, $request), $cancellation);
    }

    public function drain(
        WorkerTarget $target,
        ?WorkerControlRequest $request = null,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt {
        return $this->send($this->command(WorkerControlCommandType::Drain, $target, $request), $cancellation);
    }

    public function stop(
        WorkerTarget $target,
        ?WorkerControlRequest $request = null,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt {
        return $this->send($this->command(WorkerControlCommandType::Stop, $target, $request), $cancellation);
    }

    public function kill(
        WorkerTarget $target,
        ?WorkerControlRequest $request = null,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt {
        return $this->send($this->command(WorkerControlCommandType::Kill, $target, $request), $cancellation);
    }

    public function restart(
        WorkerTarget $target,
        ?WorkerControlRequest $request = null,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt {
        return $this->send($this->command(WorkerControlCommandType::Restart, $target, $request), $cancellation);
    }

    private function command(
        WorkerControlCommandType $type,
        WorkerTarget $target,
        ?WorkerControlRequest $request,
    ): WorkerControlCommand {
        $request ??= new WorkerControlRequest();
        $now = $this->clock->now();
        $expiresAt = $request->expiresAt;
        if ($type->isOneShot() && $expiresAt === null) {
            $expiresAt = $now->modify(sprintf('+%d seconds', $this->defaultTtlSeconds($type)));
        }

        return new WorkerControlCommand(
            $this->ids->nextCommandId($type),
            $type,
            $target,
            $now,
            $request->createdBy,
            $request->source,
            $request->reason,
            $request->requestId,
            $request->correlationId,
            $expiresAt,
            $request->idempotencyKey,
            $request->override,
        );
    }

    private function desiredState(
        WorkerControlCommand $command,
        WorkerDesiredStateType $type,
    ): WorkerDesiredState {
        return new WorkerDesiredState(
            $this->ids->nextDesiredStateId($command),
            $type,
            $command->target,
            $command->createdAt,
            $command->createdBy,
            $command->source,
            $command->reason,
            $command->requestId,
            $command->correlationId,
            $command->override,
        );
    }

    private function defaultTtlSeconds(WorkerControlCommandType $type): int
    {
        return $type === WorkerControlCommandType::Kill
            ? self::DEFAULT_KILL_COMMAND_TTL_SECONDS
            : self::DEFAULT_GRACEFUL_COMMAND_TTL_SECONDS;
    }
}
