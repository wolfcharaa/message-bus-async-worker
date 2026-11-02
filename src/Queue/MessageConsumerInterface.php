<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use Amp\Cancellation;

interface MessageConsumerInterface
{
    public function next(ConsumerOptions $options, Cancellation $cancellation): ?ReceivedQueueMessage;

    public function ack(ReceivedQueueMessage $message, Cancellation $cancellation): void;

    public function retry(ReceivedQueueMessage $message, \Throwable $reason, Cancellation $cancellation): void;

    public function reject(ReceivedQueueMessage $message, \Throwable $reason, Cancellation $cancellation): void;

    public function cancel(ReceivedQueueMessage $message, \Throwable $reason, Cancellation $cancellation): void;
}
