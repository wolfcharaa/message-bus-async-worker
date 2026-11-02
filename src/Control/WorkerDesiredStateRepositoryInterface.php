<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use Amp\Cancellation;

interface WorkerDesiredStateRepositoryInterface
{
    public function apply(WorkerDesiredState $state, Cancellation $cancellation): void;

    public function resolveFor(
        WorkerIdentity $identity,
        Cancellation $cancellation,
    ): ?WorkerDesiredState;

    /** @return list<WorkerDesiredState> */
    public function list(WorkerTarget $target, Cancellation $cancellation): array;
}
