<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

enum WorkerMode: string
{
    case Single = 'single';
    case Auto = 'auto';
}
