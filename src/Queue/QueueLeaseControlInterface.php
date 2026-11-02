<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use Amp\Cancellation;

interface QueueLeaseControlInterface
{
    public function heartbeat(ReceivedQueueMessage $message, Cancellation $cancellation): void;

    public function isCancellationRequested(ReceivedQueueMessage $message, Cancellation $cancellation): bool;

    public function recoverStale(ConsumerOptions $options, Cancellation $cancellation): int;
}
