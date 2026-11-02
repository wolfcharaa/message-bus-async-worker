<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

enum WorkerActivityState: string
{
    case Idle = 'idle';
    case Busy = 'busy';
}
