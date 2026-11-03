<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Cli;

use Fedor\MessageBus\Worker\Runtime\WorkerRuntime;
use RuntimeException;

final readonly class BootstrapResolver
{
    /** @param list<string> $conventionPaths */
    public function __construct(
        private array $conventionPaths = [
            'message-bus-worker.php',
            'config/message-bus-worker.php',
            'config/message_bus_worker.php',
        ],
    ) {
    }

    public function resolve(?string $bootstrapPath = null): WorkerRuntime
    {
        $path = $bootstrapPath ?? $this->environmentPath() ?? $this->conventionPath();

        if ($path === null || !is_file($path)) {
            throw new RuntimeException(
                'Worker bootstrap file was not found. Pass --bootstrap or set MESSAGE_BUS_WORKER_BOOTSTRAP.',
            );
        }

        $runtime = require $path;
        if (!$runtime instanceof WorkerRuntime) {
            throw new RuntimeException('Worker bootstrap must return ' . WorkerRuntime::class . '.');
        }

        return $runtime;
    }

    private function environmentPath(): ?string
    {
        $path = getenv('MESSAGE_BUS_WORKER_BOOTSTRAP');

        return is_string($path) && $path !== '' ? $path : null;
    }

    private function conventionPath(): ?string
    {
        foreach ($this->conventionPaths as $path) {
            if (is_file($path)) {
                return $path;
            }
        }

        return null;
    }
}
