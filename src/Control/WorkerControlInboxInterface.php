<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use Amp\Cancellation;

interface WorkerControlInboxInterface
{
    public function receive(
        WorkerIdentity $identity,
        WorkerControlCursor $cursor,
        Cancellation $cancellation,
    ): WorkerControlBatch;
}
