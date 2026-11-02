<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use Amp\Cancellation;

interface QueueWorkerInterface
{
    public function handle(ReceivedQueueMessage $message, Cancellation $cancellation): void;
}
