<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use RuntimeException;

final class GracefulShutdownExpired extends RuntimeException
{
    public static function forMessage(ReceivedQueueMessage $message): self
    {
        return new self(sprintf(
            'Graceful shutdown timeout expired while handling queue message `%s`.',
            $message->queueMessageId,
        ));
    }
}
