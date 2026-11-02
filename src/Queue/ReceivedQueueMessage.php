<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use Fedor\MessageBus\Queue\QueueMessage;
use InvalidArgumentException;

final readonly class ReceivedQueueMessage
{
    public function __construct(
        public string $queueMessageId,
        public QueueMessage $message,
        public int $attempts,
        public string $leaseOwner,
    ) {
        if (trim($this->queueMessageId) === '') {
            throw new InvalidArgumentException('Queue message ID must not be empty.');
        }
        if ($this->attempts < 1) {
            throw new InvalidArgumentException('Received queue message attempts must be positive.');
        }
        if (trim($this->leaseOwner) === '') {
            throw new InvalidArgumentException('Queue lease owner must not be empty.');
        }
    }

    public function canRetry(): bool
    {
        return $this->attempts < $this->message->retryPolicySnapshot->maxAttempts;
    }
}
