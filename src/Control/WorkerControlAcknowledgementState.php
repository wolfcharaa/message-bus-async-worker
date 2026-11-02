<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

enum WorkerControlAcknowledgementState: string
{
    case Applied = 'applied';
    case Skipped = 'skipped';
    case Failed = 'failed';
    case Expired = 'expired';
}
