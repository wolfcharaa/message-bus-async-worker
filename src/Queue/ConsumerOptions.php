<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use InvalidArgumentException;

final readonly class ConsumerOptions
{
    /**
     * @param list<string> $flows
     * @param list<string> $bindingIds
     * @param list<string> $bindingPatterns
     */
    public function __construct(
        public string $transport,
        public string $queue,
        public int $timeoutSeconds = 5,
        public int $limit = 1,
        public string $workerId = 'message-bus-worker',
        public int $lockTtlSeconds = 300,
        public array $flows = [],
        public array $bindingIds = [],
        public array $bindingPatterns = [],
        public int $staleRecoveryBatchSize = 100,
    ) {
        self::nonEmpty($this->transport, 'Transport');
        self::nonEmpty($this->queue, 'Queue');
        self::nonEmpty($this->workerId, 'Worker ID');
        if ($this->timeoutSeconds < 0) {
            throw new InvalidArgumentException('Consumer timeout must not be negative.');
        }
        if ($this->limit < 1) {
            throw new InvalidArgumentException('Consumer limit must be positive.');
        }
        if ($this->lockTtlSeconds < 1) {
            throw new InvalidArgumentException('Consumer lock TTL must be positive.');
        }
        if ($this->staleRecoveryBatchSize < 1) {
            throw new InvalidArgumentException('Stale recovery batch size must be positive.');
        }

        self::nonEmptyList($this->flows, 'Flow filter');
        self::nonEmptyList($this->bindingIds, 'Binding ID filter');
        self::nonEmptyList($this->bindingPatterns, 'Binding pattern filter');
    }

    private static function nonEmpty(string $value, string $name): void
    {
        if (trim($value) === '') {
            throw new InvalidArgumentException($name . ' must not be empty.');
        }
    }

    /** @param list<string> $values */
    private static function nonEmptyList(array $values, string $name): void
    {
        foreach ($values as $value) {
            self::nonEmpty($value, $name);
        }
    }
}
