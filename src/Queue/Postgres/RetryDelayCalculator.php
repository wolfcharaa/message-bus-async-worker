<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue\Postgres;

use Fedor\MessageBus\Queue\RetryPolicySnapshot;

final class RetryDelayCalculator
{
    public function seconds(RetryPolicySnapshot $snapshot, int $attempt): int
    {
        if ($snapshot->strategy === 'fixed') {
            return (int) ($snapshot->parameters['delaySeconds'] ?? 60);
        }

        $initial = (int) ($snapshot->parameters['initialDelaySeconds'] ?? 30);
        $multiplier = (float) ($snapshot->parameters['multiplier'] ?? 2.0);
        $maximum = array_key_exists('maxDelaySeconds', $snapshot->parameters)
            ? $snapshot->parameters['maxDelaySeconds']
            : 300;
        $delay = (int) round($initial * ($multiplier ** max(0, $attempt - 1)));

        return $maximum === null ? $delay : min($delay, (int) $maximum);
    }
}
