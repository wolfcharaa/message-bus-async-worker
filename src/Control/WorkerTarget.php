<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use InvalidArgumentException;

final readonly class WorkerTarget
{
    /**
     * @param list<string> $flows
     * @param list<string> $bindingIds
     * @param list<string> $bindingPatterns
     */
    public function __construct(
        public ?string $workerId = null,
        public ?string $workerName = null,
        public ?string $workerInstanceId = null,
        public ?string $workerGroup = null,
        public ?string $transport = null,
        public ?string $queue = null,
        public array $flows = [],
        public array $bindingIds = [],
        public array $bindingPatterns = [],
        public ?WorkerMode $mode = null,
        public ?string $host = null,
        public bool $all = false,
    ) {
        if ($this->all && $this->hasFilters()) {
            throw new InvalidArgumentException('Global worker target cannot be combined with other filters.');
        }
        if (!$this->all && !$this->hasFilters()) {
            throw new InvalidArgumentException('Worker target must define at least one filter or explicit all scope.');
        }
    }

    public static function all(): self
    {
        return new self(all: true);
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return [
            'workerId' => $this->workerId,
            'workerName' => $this->workerName,
            'workerInstanceId' => $this->workerInstanceId,
            'workerGroup' => $this->workerGroup,
            'transport' => $this->transport,
            'queue' => $this->queue,
            'flows' => $this->flows,
            'bindingIds' => $this->bindingIds,
            'bindingPatterns' => $this->bindingPatterns,
            'mode' => $this->mode?->value,
            'host' => $this->host,
            'all' => $this->all,
        ];
    }

    /** @param array<string, mixed> $data */
    public static function fromArray(array $data): self
    {
        $mode = self::nullableString($data, 'mode');
        $all = $data['all'] ?? false;
        if (!is_bool($all)) {
            throw new InvalidArgumentException('Worker target field `all` must be a boolean.');
        }

        return new self(
            workerId: self::nullableString($data, 'workerId'),
            workerName: self::nullableString($data, 'workerName'),
            workerInstanceId: self::nullableString($data, 'workerInstanceId'),
            workerGroup: self::nullableString($data, 'workerGroup'),
            transport: self::nullableString($data, 'transport'),
            queue: self::nullableString($data, 'queue'),
            flows: self::stringList($data, 'flows'),
            bindingIds: self::stringList($data, 'bindingIds'),
            bindingPatterns: self::stringList($data, 'bindingPatterns'),
            mode: $mode !== null ? WorkerMode::from($mode) : null,
            host: self::nullableString($data, 'host'),
            all: $all,
        );
    }

    public function matches(WorkerIdentity $identity): bool
    {
        if ($this->all) {
            return true;
        }
        if ($this->workerId !== null && $identity->workerId !== $this->workerId) {
            return false;
        }
        if ($this->workerName !== null && $identity->workerName !== $this->workerName) {
            return false;
        }
        if ($this->workerInstanceId !== null && $identity->workerInstanceId !== $this->workerInstanceId) {
            return false;
        }
        if ($this->workerGroup !== null && $identity->workerGroup !== $this->workerGroup) {
            return false;
        }
        if ($this->transport !== null && $identity->transport !== $this->transport) {
            return false;
        }
        if ($this->queue !== null && $identity->queue !== $this->queue) {
            return false;
        }
        if ($this->mode !== null && $identity->mode !== $this->mode) {
            return false;
        }
        if ($this->host !== null && $identity->host !== $this->host) {
            return false;
        }
        if (!$this->matchesListFilters($this->flows, $identity->flows)) {
            return false;
        }

        return $this->matchesBindingFilters($identity);
    }

    public function specificityScore(): int
    {
        if ($this->all) {
            return 0;
        }

        return ($this->workerInstanceId !== null ? 1000 : 0)
            + ($this->workerId !== null ? 600 : 0)
            + ($this->workerName !== null ? 500 : 0)
            + ($this->host !== null ? 300 : 0)
            + ($this->workerGroup !== null ? 200 : 0)
            + ($this->mode !== null ? 100 : 0)
            + ($this->transport !== null ? 80 : 0)
            + ($this->queue !== null ? 80 : 0)
            + count($this->flows) * 40
            + count($this->bindingIds) * 60
            + count($this->bindingPatterns) * 30;
    }

    private function hasFilters(): bool
    {
        return $this->workerId !== null
            || $this->workerName !== null
            || $this->workerInstanceId !== null
            || $this->workerGroup !== null
            || $this->transport !== null
            || $this->queue !== null
            || $this->flows !== []
            || $this->bindingIds !== []
            || $this->bindingPatterns !== []
            || $this->mode !== null
            || $this->host !== null;
    }

    /** @param list<string> $targetValues
     * @param list<string> $workerValues
     */
    private function matchesListFilters(array $targetValues, array $workerValues): bool
    {
        if ($targetValues === [] || $workerValues === []) {
            return true;
        }
        return array_intersect($targetValues, $workerValues) !== [];
    }

    private function matchesBindingFilters(WorkerIdentity $identity): bool
    {
        if ($this->bindingIds === [] && $this->bindingPatterns === []) {
            return true;
        }
        if ($identity->bindingIds === [] && $identity->bindingPatterns === []) {
            return true;
        }
        if (array_intersect($this->bindingIds, $identity->bindingIds) !== []) {
            return true;
        }
        foreach ($this->bindingIds as $bindingId) {
            foreach ($identity->bindingPatterns as $pattern) {
                if ($this->matchesPattern($pattern, $bindingId)) {
                    return true;
                }
            }
        }
        foreach ($this->bindingPatterns as $pattern) {
            foreach ($identity->bindingIds as $bindingId) {
                if ($this->matchesPattern($pattern, $bindingId)) {
                    return true;
                }
            }
        }

        return array_intersect($this->bindingPatterns, $identity->bindingPatterns) !== [];
    }

    private function matchesPattern(string $pattern, string $value): bool
    {
        $regex = '/^' . str_replace('\\*', '.*', preg_quote($pattern, '/')) . '$/';
        return preg_match($regex, $value) === 1;
    }

    /** @param array<string, mixed> $data */
    private static function nullableString(array $data, string $key): ?string
    {
        $value = $data[$key] ?? null;
        if ($value !== null && !is_string($value)) {
            throw new InvalidArgumentException(sprintf('Worker target field `%s` must be a string or null.', $key));
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
            throw new InvalidArgumentException(sprintf('Worker target field `%s` must be a list.', $key));
        }
        foreach ($value as $item) {
            if (!is_string($item)) {
                throw new InvalidArgumentException(sprintf('Worker target field `%s` must contain strings.', $key));
            }
        }
        return $value;
    }
}
