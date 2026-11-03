<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue;

use InvalidArgumentException;

final readonly class QueueWorkerRunnerOptions
{
    public function __construct(
        public ?int $maxMessages = null,
        public ?int $maxRuntimeSeconds = null,
        public ?int $idleTimeoutSeconds = null,
        public bool $stopWhenEmpty = false,
        public int $sleepWhenIdleMilliseconds = 500,
        public ?int $memoryLimitBytes = null,
        public int $heartbeatIntervalMilliseconds = 10_000,
        public int $cancellationPollIntervalMilliseconds = 500,
        public int $gracefulShutdownTimeoutSeconds = 30,
        public ?string $workerName = null,
        public ?string $workerGroup = null,
        public ?string $workerInstanceId = null,
        public ?string $host = null,
        public int $controlPollIntervalMilliseconds = 1_000,
        public int $restartExitCode = 75,
    ) {
        self::positiveOrNull($this->maxMessages, 'Maximum messages');
        self::positiveOrNull($this->maxRuntimeSeconds, 'Maximum runtime');
        self::positiveOrNull($this->idleTimeoutSeconds, 'Idle timeout');
        self::positiveOrNull($this->memoryLimitBytes, 'Memory limit');

        if ($this->sleepWhenIdleMilliseconds < 0) {
            throw new InvalidArgumentException('Idle sleep must not be negative.');
        }
        if ($this->heartbeatIntervalMilliseconds < 1) {
            throw new InvalidArgumentException('Heartbeat interval must be positive.');
        }
        if ($this->cancellationPollIntervalMilliseconds < 1) {
            throw new InvalidArgumentException('Cancellation poll interval must be positive.');
        }
        if ($this->gracefulShutdownTimeoutSeconds < 1) {
            throw new InvalidArgumentException('Graceful shutdown timeout must be positive.');
        }
        if ($this->controlPollIntervalMilliseconds < 1) {
            throw new InvalidArgumentException('Control poll interval must be positive.');
        }
    }

    private static function positiveOrNull(?int $value, string $name): void
    {
        if ($value !== null && $value < 1) {
            throw new InvalidArgumentException($name . ' must be positive when configured.');
        }
    }
}
