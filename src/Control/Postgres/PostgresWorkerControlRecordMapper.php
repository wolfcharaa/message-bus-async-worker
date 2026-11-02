<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control\Postgres;

use DateTimeImmutable;
use DateTimeZone;
use Fedor\MessageBus\Worker\Control\WorkerControlCommand;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandType;
use Fedor\MessageBus\Worker\Control\WorkerDesiredState;
use Fedor\MessageBus\Worker\Control\WorkerDesiredStateType;
use Fedor\MessageBus\Worker\Control\WorkerMode;
use Fedor\MessageBus\Worker\Control\WorkerTarget;
use JsonException;
use RuntimeException;

final class PostgresWorkerControlRecordMapper
{
    /** @param array<string, mixed> $row */
    public function command(array $row): WorkerControlCommand
    {
        return new WorkerControlCommand(
            $this->string($row, 'command_id'),
            WorkerControlCommandType::from($this->string($row, 'type')),
            $this->target($row),
            $this->date($this->field($row, 'created_at')),
            $this->nullableString($row, 'created_by'),
            $this->string($row, 'source'),
            $this->nullableString($row, 'reason'),
            $this->nullableString($row, 'request_id'),
            $this->nullableString($row, 'correlation_id'),
            $this->nullableDate($this->field($row, 'expires_at')),
            $this->nullableString($row, 'idempotency_key'),
            $this->boolean($row, 'override'),
        );
    }

    /** @param array<string, mixed> $row */
    public function desiredState(array $row): WorkerDesiredState
    {
        return new WorkerDesiredState(
            $this->string($row, 'desired_state_id'),
            WorkerDesiredStateType::from($this->string($row, 'state')),
            $this->target($row),
            $this->date($this->field($row, 'created_at')),
            $this->nullableString($row, 'created_by'),
            $this->string($row, 'source'),
            $this->nullableString($row, 'reason'),
            $this->nullableString($row, 'request_id'),
            $this->nullableString($row, 'correlation_id'),
            $this->boolean($row, 'override'),
            $this->boolean($row, 'active'),
        );
    }

    /** @param array<string, mixed> $row */
    public function integer(array $row, string $field): int
    {
        $value = $this->field($row, $field);
        if (is_int($value)) {
            return $value;
        }
        if (is_string($value) && preg_match('/^-?\d+$/', $value) === 1) {
            return (int) $value;
        }

        throw $this->invalid($field, 'an integer');
    }

    /** @param array<string, mixed> $row */
    private function target(array $row): WorkerTarget
    {
        $mode = $this->nullableString($row, 'target_mode');

        return new WorkerTarget(
            workerId: $this->nullableString($row, 'target_worker_id'),
            workerName: $this->nullableString($row, 'target_worker_name'),
            workerInstanceId: $this->nullableString($row, 'target_worker_instance_id'),
            workerGroup: $this->nullableString($row, 'target_worker_group'),
            transport: $this->nullableString($row, 'target_transport'),
            queue: $this->nullableString($row, 'target_queue'),
            flows: $this->stringList($row, 'target_flows'),
            bindingIds: $this->stringList($row, 'target_binding_ids'),
            bindingPatterns: $this->stringList($row, 'target_binding_patterns'),
            mode: $mode === null ? null : WorkerMode::from($mode),
            host: $this->nullableString($row, 'target_host'),
            all: $this->boolean($row, 'target_all'),
        );
    }

    /** @param array<string, mixed> $row */
    private function field(array $row, string $field): mixed
    {
        if (!array_key_exists($field, $row)) {
            throw new RuntimeException(sprintf('PostgreSQL worker control row is missing field `%s`.', $field));
        }

        return $row[$field];
    }

    /** @param array<string, mixed> $row */
    private function string(array $row, string $field): string
    {
        $value = $this->field($row, $field);
        if (!is_string($value)) {
            throw $this->invalid($field, 'a string');
        }

        return $value;
    }

    /** @param array<string, mixed> $row */
    private function nullableString(array $row, string $field): ?string
    {
        $value = $this->field($row, $field);
        if ($value !== null && !is_string($value)) {
            throw $this->invalid($field, 'a string or null');
        }

        return $value;
    }

    /** @param array<string, mixed> $row */
    private function boolean(array $row, string $field): bool
    {
        $value = $this->field($row, $field);
        if (is_bool($value)) {
            return $value;
        }
        if (is_int($value) && ($value === 0 || $value === 1)) {
            return $value === 1;
        }
        if (is_string($value)) {
            return match (strtolower($value)) {
                '1', 't', 'true' => true,
                '0', 'f', 'false' => false,
                default => throw $this->invalid($field, 'a boolean'),
            };
        }

        throw $this->invalid($field, 'a boolean');
    }

    /**
     * @param array<string, mixed> $row
     * @return list<string>
     */
    private function stringList(array $row, string $field): array
    {
        $value = $this->field($row, $field);
        if (is_string($value)) {
            try {
                $value = json_decode($value, true, 512, JSON_THROW_ON_ERROR);
            } catch (JsonException $error) {
                throw new RuntimeException(sprintf(
                    'PostgreSQL worker control field `%s` contains invalid JSON.',
                    $field,
                ), previous: $error);
            }
        }
        if (!is_array($value) || !array_is_list($value)) {
            throw $this->invalid($field, 'a JSON string list');
        }
        foreach ($value as $item) {
            if (!is_string($item)) {
                throw $this->invalid($field, 'a JSON string list');
            }
        }

        return $value;
    }

    private function date(mixed $value): DateTimeImmutable
    {
        if ($value instanceof DateTimeImmutable) {
            return $value;
        }
        if (!is_string($value)) {
            throw new RuntimeException('PostgreSQL worker control date must be a string.');
        }

        return new DateTimeImmutable($value, new DateTimeZone('UTC'));
    }

    private function nullableDate(mixed $value): ?DateTimeImmutable
    {
        return $value === null ? null : $this->date($value);
    }

    private function invalid(string $field, string $expected): RuntimeException
    {
        return new RuntimeException(sprintf(
            'PostgreSQL worker control field `%s` must be %s.',
            $field,
            $expected,
        ));
    }
}
