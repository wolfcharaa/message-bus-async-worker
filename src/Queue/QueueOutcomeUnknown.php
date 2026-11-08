<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use Fedor\MessageBus\Exception\NonRetryableMessageExceptionInterface;
use RuntimeException;
use Throwable;

final class QueueOutcomeUnknown extends RuntimeException implements NonRetryableMessageExceptionInterface
{
    public static function forMessage(ReceivedQueueMessage $message, Throwable $reason): self
    {
        return new self(
            sprintf(
                'Queue message `%s` stopped with an unknown side-effect outcome; automatic retry is forbidden.',
                $message->queueMessageId,
            ),
            previous: $reason,
        );
    }
}
