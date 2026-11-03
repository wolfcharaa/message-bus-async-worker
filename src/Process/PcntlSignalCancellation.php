<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Process;

use Amp\Cancellation;
use Amp\DeferredCancellation;

final class PcntlSignalCancellation
{
    private readonly DeferredCancellation $deferredCancellation;

    /** @var array<int, callable|int> */
    private array $previousHandlers = [];

    private ?bool $previousAsyncSignals = null;

    private bool $closed = false;

    /** @param list<int>|null $signals */
    public function __construct(?array $signals = null)
    {
        $this->deferredCancellation = new DeferredCancellation();

        if (!extension_loaded('pcntl')) {
            return;
        }

        $signals ??= [SIGTERM, SIGINT, SIGQUIT];
        $this->previousAsyncSignals = pcntl_async_signals();
        pcntl_async_signals(true);

        foreach ($signals as $signal) {
            $this->previousHandlers[$signal] = pcntl_signal_get_handler($signal);
            pcntl_signal($signal, function (): void {
                if (!$this->deferredCancellation->isCancelled()) {
                    $this->deferredCancellation->cancel();
                }
            });
        }
    }

    public function cancellation(): Cancellation
    {
        return $this->deferredCancellation->getCancellation();
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;

        if ($this->previousAsyncSignals === null) {
            return;
        }

        foreach ($this->previousHandlers as $signal => $handler) {
            pcntl_signal($signal, $handler);
        }
        pcntl_async_signals($this->previousAsyncSignals);
        $this->previousHandlers = [];
    }

    public function __destruct()
    {
        $this->close();
    }
}
