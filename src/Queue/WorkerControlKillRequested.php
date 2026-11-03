<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use RuntimeException;

final class WorkerControlKillRequested extends RuntimeException
{
    public static function forMessage(ReceivedQueueMessage $message): self
    {
        return new self(sprintf(
            'Worker kill was requested while handling queue message `%s`.',
            $message->queueMessageId,
        ));
    }
}
