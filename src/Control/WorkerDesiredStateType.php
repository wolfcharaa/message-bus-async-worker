<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

enum WorkerDesiredStateType: string
{
    case Paused = 'paused';
    case Resumed = 'resumed';
}
