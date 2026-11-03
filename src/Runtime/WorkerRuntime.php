<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Runtime;

use Fedor\MessageBus\Worker\Control\WorkerControlRuntime;
use Fedor\MessageBus\Worker\Queue\QueueWorkerRunner;
use RuntimeException;

final readonly class WorkerRuntime
{
    public function __construct(
        public QueueWorkerRunner $runner,
        public ?WorkerControlRuntime $control = null,
    ) {
    }

    public function requireControl(): WorkerControlRuntime
    {
        return $this->control
            ?? throw new RuntimeException('Worker control runtime is not configured by the bootstrap.');
    }
}
