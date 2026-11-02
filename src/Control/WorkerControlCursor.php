<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

final readonly class WorkerControlCursor
{
    public function __construct(public ?string $lastCommandId = null)
    {
    }
}
