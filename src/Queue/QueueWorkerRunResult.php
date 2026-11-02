<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

final readonly class QueueWorkerRunResult
{
    public function __construct(
        public int $handled = 0,
        public int $succeeded = 0,
        public int $retried = 0,
        public int $rejected = 0,
        public int $cancelled = 0,
        public int $leaseLost = 0,
    ) {
    }
}
