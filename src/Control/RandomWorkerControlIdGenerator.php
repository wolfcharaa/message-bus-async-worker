<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

final readonly class RandomWorkerControlIdGenerator implements WorkerControlIdGeneratorInterface
{
    public function nextCommandId(WorkerControlCommandType $type): string
    {
        return 'worker_command.' . $type->value . '.' . bin2hex(random_bytes(16));
    }

    public function nextDesiredStateId(WorkerControlCommand $command): string
    {
        return 'worker_desired_state.' . $command->commandId;
    }
}
