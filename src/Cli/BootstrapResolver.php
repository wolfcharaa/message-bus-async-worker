<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Cli;

use Closure;
use Fedor\MessageBus\Worker\Runtime\WorkerRuntime;
use Fedor\MessageBus\Worker\Runtime\WorkerRuntimeProviderInterface;
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
        $bootstrap = $this->load($bootstrapPath);
        if ($bootstrap instanceof WorkerRuntimeProviderInterface) {
            throw new RuntimeException(
                'Managed worker bootstrap cannot be resolved outside its lifecycle; use BootstrapResolver::run().',
            );
        }

        return $bootstrap;
    }

    /**
     * @template TResult
     * @param Closure(WorkerRuntime): TResult $operation
     * @return TResult
     */
    public function run(?string $bootstrapPath, Closure $operation): mixed
    {
        $bootstrap = $this->load($bootstrapPath);

        return $bootstrap instanceof WorkerRuntimeProviderInterface
            ? $bootstrap->run($operation)
            : $operation($bootstrap);
    }

    private function load(?string $bootstrapPath): WorkerRuntime|WorkerRuntimeProviderInterface
    {
        $path = $bootstrapPath ?? $this->environmentPath() ?? $this->conventionPath();

        if ($path === null || !is_file($path)) {
            throw new RuntimeException(
                'Worker bootstrap file was not found. Pass --bootstrap or set MESSAGE_BUS_WORKER_BOOTSTRAP.',
            );
        }

        $bootstrap = require $path;
        if (!$bootstrap instanceof WorkerRuntime && !$bootstrap instanceof WorkerRuntimeProviderInterface) {
            throw new RuntimeException(sprintf(
                'Worker bootstrap must return %s or %s.',
                WorkerRuntime::class,
                WorkerRuntimeProviderInterface::class,
            ));
        }

        return $bootstrap;
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
