<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use Amp\Cancellation;
use Amp\NullCancellation;

interface WorkerControlServiceInterface
{
    public function send(
        WorkerControlCommand $command,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt;

    public function pause(
        WorkerTarget $target,
        ?WorkerControlRequest $request = null,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt;

    public function resume(
        WorkerTarget $target,
        ?WorkerControlRequest $request = null,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt;

    public function drain(
        WorkerTarget $target,
        ?WorkerControlRequest $request = null,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt;

    public function stop(
        WorkerTarget $target,
        ?WorkerControlRequest $request = null,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt;

    public function kill(
        WorkerTarget $target,
        ?WorkerControlRequest $request = null,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt;

    public function restart(
        WorkerTarget $target,
        ?WorkerControlRequest $request = null,
        Cancellation $cancellation = new NullCancellation(),
    ): WorkerControlCommandReceipt;
}
