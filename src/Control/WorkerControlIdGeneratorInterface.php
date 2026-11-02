<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

interface WorkerControlIdGeneratorInterface
{
    public function nextCommandId(WorkerControlCommandType $type): string;

    public function nextDesiredStateId(WorkerControlCommand $command): string;
}
