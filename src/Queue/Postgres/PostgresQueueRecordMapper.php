<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Queue\Postgres;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Fedor\MessageBus\Envelope\SerializedEnvelope;
use Fedor\MessageBus\Queue\QueueMessage;
use Fedor\MessageBus\Queue\RetryPolicySnapshot;
use Fedor\MessageBus\Worker\Queue\ReceivedQueueMessage;
use JsonException;
use RuntimeException;

final class PostgresQueueRecordMapper
{
    /** @param array<string, mixed> $row */
    public function received(array $row): ReceivedQueueMessage
    {
        $serialized = $this->json($this->field($row, 'serialized_envelope'), 'serialized_envelope');
        $retry = $this->json($this->field($row, 'retry_policy_snapshot'), 'retry_policy_snapshot');
        $parameters = $retry['parameters'] ?? [];
        if (!is_array($parameters) || array_is_list($parameters)) {
            throw new RuntimeException('PostgreSQL retry policy parameters must be an object.');
        }
        $parameters = $this->jsonObject($parameters, 'retry_policy_snapshot.parameters');

        return new ReceivedQueueMessage(
            $this->identifier($row, 'id'),
            new QueueMessage(
                $this->string($row, 'transport'),
                $this->string($row, 'queue'),
                new SerializedEnvelope(
                    $this->arrayString($serialized, 'messageName'),
                    $this->arrayString($serialized, 'payload'),
                    $this->arrayInteger($serialized, 'schemaVersion'),
                ),
                $this->string($row, 'message_id'),
                $this->string($row, 'correlation_id'),
                $this->string($row, 'flow'),
                $this->string($row, 'binding_id'),
                $this->date($this->field($row, 'available_at')),
                $this->integer($row, 'priority'),
                $this->string($row, 'retry_policy_key'),
                new RetryPolicySnapshot(
                    $this->arrayInteger($retry, 'maxAttempts'),
                    $this->arrayString($retry, 'strategy'),
                    $this->retryParameters($parameters),
                ),
            ),
            $this->integer($row, 'attempts'),
            $this->string($row, 'locked_by'),
        );
    }

    /** @param array<string, mixed> $row */
    public function identifier(array $row, string $field): string
    {
        $value = $this->field($row, $field);
        if (!is_string($value) && !is_int($value)) {
            throw new RuntimeException(sprintf('PostgreSQL queue field `%s` must be an identifier.', $field));
        }

        return (string) $value;
    }

    /** @param array<string, mixed> $row */
    public function boolean(array $row, string $field): bool
    {
        $value = $this->field($row, $field);
        if (!is_bool($value)) {
            throw new RuntimeException(sprintf('PostgreSQL queue field `%s` must be a boolean.', $field));
        }

        return $value;
    }

    /**
     * @param array<string, mixed> $parameters
     * @return array<string, int|float|string|null>
     */
    private function retryParameters(array $parameters): array
    {
        $result = [];
        foreach ($parameters as $key => $value) {
            if (!is_int($value) && !is_float($value) && !is_string($value) && $value !== null) {
                throw new RuntimeException('PostgreSQL retry policy parameters contain an invalid value.');
            }
            $result[$key] = $value;
        }

        return $result;
    }

    /** @return array<string, mixed> */
    private function json(mixed $value, string $field): array
    {
        if (is_array($value) && !array_is_list($value)) {
            return $this->jsonObject($value, $field);
        }
        if (!is_string($value)) {
            throw new RuntimeException(sprintf('PostgreSQL queue field `%s` must contain a JSON object.', $field));
        }

        try {
            $decoded = json_decode($value, true, flags: JSON_THROW_ON_ERROR | JSON_BIGINT_AS_STRING);
        } catch (JsonException $error) {
            throw new RuntimeException(sprintf('PostgreSQL queue field `%s` contains invalid JSON.', $field), previous: $error);
        }
        if (!is_array($decoded) || array_is_list($decoded)) {
            throw new RuntimeException(sprintf('PostgreSQL queue field `%s` must contain a JSON object.', $field));
        }

        return $this->jsonObject($decoded, $field);
    }

    /**
     * @param array<mixed, mixed> $value
     * @return array<string, mixed>
     */
    private function jsonObject(array $value, string $field): array
    {
        $object = [];
        foreach ($value as $key => $item) {
            if (!is_string($key)) {
                throw new RuntimeException(sprintf(
                    'PostgreSQL queue field `%s` must contain a JSON object with string keys.',
                    $field,
                ));
            }
            $object[$key] = $item;
        }

        return $object;
    }

    /** @param array<string, mixed> $row */
    private function field(array $row, string $field): mixed
    {
        if (!array_key_exists($field, $row)) {
            throw new RuntimeException(sprintf('PostgreSQL queue row is missing field `%s`.', $field));
        }

        return $row[$field];
    }

    /** @param array<string, mixed> $row */
    private function string(array $row, string $field): string
    {
        $value = $this->field($row, $field);
        if (!is_string($value)) {
            throw new RuntimeException(sprintf('PostgreSQL queue field `%s` must be a string.', $field));
        }

        return $value;
    }

    /** @param array<string, mixed> $row */
    private function integer(array $row, string $field): int
    {
        $value = $this->field($row, $field);
        if (!is_int($value)) {
            throw new RuntimeException(sprintf('PostgreSQL queue field `%s` must be an integer.', $field));
        }

        return $value;
    }

    /** @param array<string, mixed> $value */
    private function arrayString(array $value, string $field): string
    {
        $fieldValue = $value[$field] ?? null;
        if (!is_string($fieldValue)) {
            throw new RuntimeException(sprintf('PostgreSQL queue JSON field `%s` must be a string.', $field));
        }

        return $fieldValue;
    }

    /** @param array<string, mixed> $value */
    private function arrayInteger(array $value, string $field): int
    {
        $fieldValue = $value[$field] ?? null;
        if (!is_int($fieldValue)) {
            throw new RuntimeException(sprintf('PostgreSQL queue JSON field `%s` must be an integer.', $field));
        }

        return $fieldValue;
    }

    private function date(mixed $value): DateTimeImmutable
    {
        if ($value instanceof DateTimeImmutable) {
            return $value;
        }
        if ($value instanceof DateTimeInterface) {
            return DateTimeImmutable::createFromInterface($value);
        }
        if (!is_string($value)) {
            throw new RuntimeException('PostgreSQL queue timestamp has an invalid type.');
        }

        return new DateTimeImmutable($value, new DateTimeZone('UTC'));
    }
}
