<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use RuntimeException;

final class JobRuntimeExpired extends RuntimeException
{
    public static function forMessage(ReceivedQueueMessage $message, int $maxRuntimeSeconds): self
    {
        return new self(sprintf(
            'Maximum runtime of %d seconds expired for queue message `%s`.',
            $maxRuntimeSeconds,
            $message->queueMessageId,
        ));
    }
}
