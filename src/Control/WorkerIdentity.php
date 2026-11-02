<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use DateTimeImmutable;
use InvalidArgumentException;

final readonly class WorkerIdentity
{
    /**
     * @param list<string> $flows
     * @param list<string> $bindingIds
     * @param list<string> $bindingPatterns
     */
    public function __construct(
        public string $workerName,
        public string $workerInstanceId,
        public ?string $workerGroup,
        public string $host,
        public int $pid,
        public DateTimeImmutable $startedAt,
        public WorkerMode $mode,
        public string $transport,
        public string $queue,
        public array $flows = [],
        public array $bindingIds = [],
        public array $bindingPatterns = [],
        public ?string $workerId = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return [
            'workerName' => $this->workerName,
            'workerInstanceId' => $this->workerInstanceId,
            'workerGroup' => $this->workerGroup,
            'host' => $this->host,
            'pid' => $this->pid,
            'startedAt' => $this->startedAt->format(DATE_ATOM),
            'mode' => $this->mode->value,
            'transport' => $this->transport,
            'queue' => $this->queue,
            'flows' => $this->flows,
            'bindingIds' => $this->bindingIds,
            'bindingPatterns' => $this->bindingPatterns,
            'workerId' => $this->workerId,
        ];
    }

    /** @param array<string, mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            self::string($data, 'workerName'),
            self::string($data, 'workerInstanceId'),
            self::nullableString($data, 'workerGroup'),
            self::string($data, 'host'),
            self::integer($data, 'pid'),
            new DateTimeImmutable(self::string($data, 'startedAt')),
            WorkerMode::from(self::string($data, 'mode')),
            self::string($data, 'transport'),
            self::string($data, 'queue'),
            self::stringList($data, 'flows'),
            self::stringList($data, 'bindingIds'),
            self::stringList($data, 'bindingPatterns'),
            self::nullableString($data, 'workerId'),
        );
    }

    /** @param array<string, mixed> $data */
    private static function string(array $data, string $key): string
    {
        $value = $data[$key] ?? null;
        if (!is_string($value)) {
            throw new InvalidArgumentException(sprintf('Worker identity field `%s` must be a string.', $key));
        }
        return $value;
    }

    /** @param array<string, mixed> $data */
    private static function nullableString(array $data, string $key): ?string
    {
        $value = $data[$key] ?? null;
        if ($value !== null && !is_string($value)) {
            throw new InvalidArgumentException(sprintf('Worker identity field `%s` must be a string or null.', $key));
        }
        return $value;
    }

    /** @param array<string, mixed> $data */
    private static function integer(array $data, string $key): int
    {
        $value = $data[$key] ?? null;
        if (!is_int($value)) {
            throw new InvalidArgumentException(sprintf('Worker identity field `%s` must be an integer.', $key));
        }
        return $value;
    }

    /**
     * @param array<string, mixed> $data
     * @return list<string>
     */
    private static function stringList(array $data, string $key): array
    {
        $value = $data[$key] ?? [];
        if (!is_array($value) || !array_is_list($value)) {
            throw new InvalidArgumentException(sprintf('Worker identity field `%s` must be a list.', $key));
        }
        foreach ($value as $item) {
            if (!is_string($item)) {
                throw new InvalidArgumentException(sprintf('Worker identity field `%s` must contain strings.', $key));
            }
        }
        return $value;
    }
}
