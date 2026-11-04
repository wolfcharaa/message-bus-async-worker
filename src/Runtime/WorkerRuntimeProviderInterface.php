<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Runtime;

use Closure;

interface WorkerRuntimeProviderInterface
{
    /**
     * @template TResult
     * @param Closure(WorkerRuntime): TResult $operation
     * @return TResult
     */
    public function run(Closure $operation): mixed;
}
