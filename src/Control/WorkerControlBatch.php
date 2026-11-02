<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

final readonly class WorkerControlBatch
{
    /** @param list<WorkerControlCommand> $commands */
    public function __construct(
        public array $commands,
        public WorkerControlCursor $cursor,
        public ?WorkerDesiredState $desiredState = null,
    ) {
    }
}
