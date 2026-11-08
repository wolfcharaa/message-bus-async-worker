<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use RuntimeException;

final class QueueLeaseLost extends RuntimeException
{
    public static function forMessage(ReceivedQueueMessage $message): self
    {
        return new self(sprintf(
            'Queue lease `%s` attempt %d for job `%s` is no longer active.',
            $message->leaseOwner,
            $message->attempts,
            $message->queueMessageId,
        ));
    }
}
