<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use RuntimeException;

final class QueueCancellationRequested extends RuntimeException
{
    public static function forMessage(ReceivedQueueMessage $message): self
    {
        return new self(sprintf(
            'Cancellation was requested for queue message `%s`.',
            $message->queueMessageId,
        ));
    }
}
