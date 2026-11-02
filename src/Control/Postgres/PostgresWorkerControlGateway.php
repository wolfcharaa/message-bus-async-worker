<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control\Postgres;

use Amp\Cancellation;
use Amp\Postgres\PostgresLink;
use DateTimeImmutable;
use Fedor\MessageBus\Worker\Clock\UtcSystemClock;
use Fedor\MessageBus\Worker\Control\WorkerControlAcknowledgement;
use Fedor\MessageBus\Worker\Control\WorkerControlBatch;
use Fedor\MessageBus\Worker\Control\WorkerControlCommand;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandReceipt;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandRepositoryInterface;
use Fedor\MessageBus\Worker\Control\WorkerControlCursor;
use Fedor\MessageBus\Worker\Control\WorkerControlInboxInterface;
use Fedor\MessageBus\Worker\Control\WorkerControlTableDefinition;
use Fedor\MessageBus\Worker\Control\WorkerDesiredState;
use Fedor\MessageBus\Worker\Control\WorkerDesiredStateRepositoryInterface;
use Fedor\MessageBus\Worker\Control\WorkerIdentity;
use Fedor\MessageBus\Worker\Control\WorkerTarget;
use JsonException;
use Psr\Clock\ClockInterface;
use RuntimeException;

final class PostgresWorkerControlGateway implements
    WorkerControlCommandRepositoryInterface,
    WorkerDesiredStateRepositoryInterface,
    WorkerControlInboxInterface
{
    private readonly string $commandsTable;
    private readonly string $desiredStatesTable;
    private readonly string $acknowledgementsTable;
    private readonly PostgresWorkerControlRecordMapper $records;

    public function __construct(
        private readonly PostgresLink $postgres,
        private readonly ClockInterface $clock = new UtcSystemClock(),
        WorkerControlTableDefinition $definition = new WorkerControlTableDefinition(),
        ?PostgresWorkerControlRecordMapper $records = null,
    ) {
        $this->commandsTable = $this->quoteIdentifier($definition->commandsTable);
        $this->desiredStatesTable = $this->quoteIdentifier($definition->desiredStatesTable);
        $this->acknowledgementsTable = $this->quoteIdentifier($definition->acknowledgementsTable);
        $this->records = $records ?? new PostgresWorkerControlRecordMapper();
    }

    #[\Override]
    public function append(
        WorkerControlCommand $command,
        Cancellation $cancellation,
    ): WorkerControlCommandReceipt {
        $cancellation->throwIfRequested();
        $existing = $this->existingCommand($command, $cancellation);
        if ($existing !== null) {
            return $this->receipt($existing, duplicate: true);
        }

        $row = $this->postgres->execute(
            'INSERT INTO ' . $this->commandsTable . ' (
                command_id, type,
                target_worker_id, target_worker_name, target_worker_instance_id, target_worker_group,
                target_transport, target_queue, target_flows, target_binding_ids, target_binding_patterns,
                target_mode, target_host, target_all, target_specificity,
                created_at, created_by, source, reason, request_id, correlation_id,
                expires_at, idempotency_key, override
            ) VALUES (
                :command_id, :type,
                :target_worker_id, :target_worker_name, :target_worker_instance_id, :target_worker_group,
                :target_transport, :target_queue, :target_flows, :target_binding_ids, :target_binding_patterns,
                :target_mode, :target_host, :target_all, :target_specificity,
                :created_at, :created_by, :source, :reason, :request_id, :correlation_id,
                :expires_at, :idempotency_key, :override
            )
            ON CONFLICT DO NOTHING
            RETURNING *',
            $this->commandParams($command),
        )->fetchRow();
        if ($row !== null) {
            return $this->receipt($this->records->command($row), duplicate: false);
        }

        $cancellation->throwIfRequested();
        $existing = $this->existingCommand($command, $cancellation);
        if ($existing === null) {
            throw new RuntimeException('PostgreSQL worker control insert conflicted without a readable command.');
        }

        return $this->receipt($existing, duplicate: true);
    }

    #[\Override]
    public function findById(string $commandId, Cancellation $cancellation): ?WorkerControlCommand
    {
        $cancellation->throwIfRequested();
        $row = $this->postgres->execute(
            'SELECT * FROM ' . $this->commandsTable . ' WHERE command_id = :command_id',
            ['command_id' => $commandId],
        )->fetchRow();

        return $row === null ? null : $this->records->command($row);
    }

    #[\Override]
    public function findByIdempotencyKey(
        string $idempotencyKey,
        Cancellation $cancellation,
    ): ?WorkerControlCommand {
        $cancellation->throwIfRequested();
        $row = $this->postgres->execute(
            'SELECT * FROM ' . $this->commandsTable . ' WHERE idempotency_key = :idempotency_key',
            ['idempotency_key' => $idempotencyKey],
        )->fetchRow();

        return $row === null ? null : $this->records->command($row);
    }

    #[\Override]
    public function pendingFor(
        WorkerIdentity $identity,
        WorkerControlCursor $cursor,
        Cancellation $cancellation,
    ): array {
        $commands = [];
        foreach ($this->commandRowsAfter($cursor, $cancellation) as $row) {
            $command = $this->records->command($row);
            if ($command->target->matches($identity)) {
                $commands[] = $command;
            }
        }

        return $commands;
    }

    #[\Override]
    public function acknowledge(
        WorkerControlAcknowledgement $acknowledgement,
        Cancellation $cancellation,
    ): void {
        $cancellation->throwIfRequested();
        $this->postgres->execute(
            'INSERT INTO ' . $this->acknowledgementsTable . ' (
                command_id, worker_instance_id, state, acknowledged_at,
                message, error_class, error_message
            ) VALUES (
                :command_id, :worker_instance_id, :state, :acknowledged_at,
                :message, :error_class, :error_message
            )
            ON CONFLICT (command_id, worker_instance_id) DO UPDATE SET
                state = EXCLUDED.state,
                acknowledged_at = EXCLUDED.acknowledged_at,
                message = EXCLUDED.message,
                error_class = EXCLUDED.error_class,
                error_message = EXCLUDED.error_message',
            [
                'command_id' => $acknowledgement->commandId,
                'worker_instance_id' => $acknowledgement->workerInstanceId,
                'state' => $acknowledgement->state->value,
                'acknowledged_at' => $this->formatDate($acknowledgement->acknowledgedAt),
                'message' => $acknowledgement->message,
                'error_class' => $acknowledgement->errorClass,
                'error_message' => $acknowledgement->errorMessage,
            ],
        );
    }

    #[\Override]
    public function apply(WorkerDesiredState $state, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        $this->postgres->execute(
            'INSERT INTO ' . $this->desiredStatesTable . ' (
                desired_state_id, state,
                target_worker_id, target_worker_name, target_worker_instance_id, target_worker_group,
                target_transport, target_queue, target_flows, target_binding_ids, target_binding_patterns,
                target_mode, target_host, target_all, target_specificity,
                created_at, created_by, source, reason, request_id, correlation_id, override, active
            ) VALUES (
                :desired_state_id, :state,
                :target_worker_id, :target_worker_name, :target_worker_instance_id, :target_worker_group,
                :target_transport, :target_queue, :target_flows, :target_binding_ids, :target_binding_patterns,
                :target_mode, :target_host, :target_all, :target_specificity,
                :created_at, :created_by, :source, :reason, :request_id, :correlation_id, :override, :active
            )
            ON CONFLICT (desired_state_id) DO UPDATE SET
                state = EXCLUDED.state,
                target_worker_id = EXCLUDED.target_worker_id,
                target_worker_name = EXCLUDED.target_worker_name,
                target_worker_instance_id = EXCLUDED.target_worker_instance_id,
                target_worker_group = EXCLUDED.target_worker_group,
                target_transport = EXCLUDED.target_transport,
                target_queue = EXCLUDED.target_queue,
                target_flows = EXCLUDED.target_flows,
                target_binding_ids = EXCLUDED.target_binding_ids,
                target_binding_patterns = EXCLUDED.target_binding_patterns,
                target_mode = EXCLUDED.target_mode,
                target_host = EXCLUDED.target_host,
                target_all = EXCLUDED.target_all,
                target_specificity = EXCLUDED.target_specificity,
                created_at = EXCLUDED.created_at,
                created_by = EXCLUDED.created_by,
                source = EXCLUDED.source,
                reason = EXCLUDED.reason,
                request_id = EXCLUDED.request_id,
                correlation_id = EXCLUDED.correlation_id,
                override = EXCLUDED.override,
                active = EXCLUDED.active',
            $this->desiredStateParams($state),
        );
    }

    #[\Override]
    public function resolveFor(
        WorkerIdentity $identity,
        Cancellation $cancellation,
    ): ?WorkerDesiredState {
        $states = array_filter(
            $this->allDesiredStates($cancellation),
            static fn (WorkerDesiredState $state): bool => $state->matches($identity),
        );
        usort($states, static function (WorkerDesiredState $left, WorkerDesiredState $right): int {
            $specificity = $right->specificityScore() <=> $left->specificityScore();
            if ($specificity !== 0) {
                return $specificity;
            }

            return $right->createdAt <=> $left->createdAt;
        });

        return $states[0] ?? null;
    }

    #[\Override]
    public function list(WorkerTarget $target, Cancellation $cancellation): array
    {
        return array_values(array_filter(
            $this->allDesiredStates($cancellation),
            fn (WorkerDesiredState $state): bool => $this->targetsOverlap($state->target, $target),
        ));
    }

    #[\Override]
    public function receive(
        WorkerIdentity $identity,
        WorkerControlCursor $cursor,
        Cancellation $cancellation,
    ): WorkerControlBatch {
        $commands = [];
        $lastSeenCommandId = $cursor->lastCommandId;
        foreach ($this->commandRowsAfter($cursor, $cancellation) as $row) {
            $command = $this->records->command($row);
            $lastSeenCommandId = $command->commandId;
            if ($command->target->matches($identity)) {
                $commands[] = $command;
            }
        }

        return new WorkerControlBatch(
            $commands,
            new WorkerControlCursor($lastSeenCommandId),
            $this->resolveFor($identity, $cancellation),
        );
    }

    private function existingCommand(
        WorkerControlCommand $command,
        Cancellation $cancellation,
    ): ?WorkerControlCommand {
        if ($command->idempotencyKey !== null) {
            $existing = $this->findByIdempotencyKey($command->idempotencyKey, $cancellation);
            if ($existing !== null) {
                return $existing;
            }
        }

        return $this->findById($command->commandId, $cancellation);
    }

    private function receipt(WorkerControlCommand $command, bool $duplicate): WorkerControlCommandReceipt
    {
        return new WorkerControlCommandReceipt(
            $command->commandId,
            $command->type,
            $command->target,
            $command->createdAt,
            $command->idempotencyKey,
            $duplicate,
        );
    }

    /** @return array<string, bool|float|int|string|null> */
    private function commandParams(WorkerControlCommand $command): array
    {
        return $this->targetParams($command->target) + [
            'command_id' => $command->commandId,
            'type' => $command->type->value,
            'created_at' => $this->formatDate($command->createdAt),
            'created_by' => $command->createdBy,
            'source' => $command->source,
            'reason' => $command->reason,
            'request_id' => $command->requestId,
            'correlation_id' => $command->correlationId,
            'expires_at' => $command->expiresAt === null ? null : $this->formatDate($command->expiresAt),
            'idempotency_key' => $command->idempotencyKey,
            'override' => $command->override,
        ];
    }

    /** @return array<string, bool|float|int|string|null> */
    private function desiredStateParams(WorkerDesiredState $state): array
    {
        return $this->targetParams($state->target) + [
            'desired_state_id' => $state->desiredStateId,
            'state' => $state->type->value,
            'created_at' => $this->formatDate($state->createdAt),
            'created_by' => $state->createdBy,
            'source' => $state->source,
            'reason' => $state->reason,
            'request_id' => $state->requestId,
            'correlation_id' => $state->correlationId,
            'override' => $state->override,
            'active' => $state->active,
        ];
    }

    /** @return array<string, bool|float|int|string|null> */
    private function targetParams(WorkerTarget $target): array
    {
        return [
            'target_worker_id' => $target->workerId,
            'target_worker_name' => $target->workerName,
            'target_worker_instance_id' => $target->workerInstanceId,
            'target_worker_group' => $target->workerGroup,
            'target_transport' => $target->transport,
            'target_queue' => $target->queue,
            'target_flows' => $this->json($target->flows),
            'target_binding_ids' => $this->json($target->bindingIds),
            'target_binding_patterns' => $this->json($target->bindingPatterns),
            'target_mode' => $target->mode?->value,
            'target_host' => $target->host,
            'target_all' => $target->all,
            'target_specificity' => $target->specificityScore(),
        ];
    }

    /** @return list<array<string, mixed>> */
    private function commandRowsAfter(
        WorkerControlCursor $cursor,
        Cancellation $cancellation,
    ): array {
        $cancellation->throwIfRequested();
        $lastId = null;
        if ($cursor->lastCommandId !== null) {
            $row = $this->postgres->execute(
                'SELECT id FROM ' . $this->commandsTable . ' WHERE command_id = :command_id',
                ['command_id' => $cursor->lastCommandId],
            )->fetchRow();
            $lastId = $row === null ? null : $this->records->integer($row, 'id');
        }

        $where = '(expires_at IS NULL OR expires_at > :now)';
        $params = ['now' => $this->formatDate($this->clock->now())];
        if ($lastId !== null) {
            $where .= ' AND id > :last_id';
            $params['last_id'] = $lastId;
        }

        $cancellation->throwIfRequested();
        $result = $this->postgres->execute(
            'SELECT * FROM ' . $this->commandsTable . '
             WHERE ' . $where . '
             ORDER BY id ASC',
            $params,
        );

        return iterator_to_array($result, false);
    }

    /** @return list<WorkerDesiredState> */
    private function allDesiredStates(Cancellation $cancellation): array
    {
        $cancellation->throwIfRequested();
        $result = $this->postgres->query(
            'SELECT * FROM ' . $this->desiredStatesTable . '
             WHERE active = TRUE
             ORDER BY target_specificity DESC, created_at DESC',
        );
        $states = [];
        foreach ($result as $row) {
            $states[] = $this->records->desiredState($row);
        }

        return $states;
    }

    private function targetsOverlap(WorkerTarget $left, WorkerTarget $right): bool
    {
        if ($left->all || $right->all) {
            return true;
        }
        foreach ([
            'workerId',
            'workerName',
            'workerInstanceId',
            'workerGroup',
            'transport',
            'queue',
            'host',
        ] as $field) {
            if ($left->{$field} !== null && $right->{$field} !== null && $left->{$field} !== $right->{$field}) {
                return false;
            }
        }
        if ($left->mode !== null && $right->mode !== null && $left->mode !== $right->mode) {
            return false;
        }

        return $this->arraysOverlap($left->flows, $right->flows)
            && $this->arraysOverlap($left->bindingIds, $right->bindingIds)
            && $this->arraysOverlap($left->bindingPatterns, $right->bindingPatterns);
    }

    /** @param list<string> $left
     * @param list<string> $right
     */
    private function arraysOverlap(array $left, array $right): bool
    {
        return $left === [] || $right === [] || array_intersect($left, $right) !== [];
    }

    /** @param list<string> $value */
    private function json(array $value): string
    {
        try {
            return json_encode($value, JSON_THROW_ON_ERROR);
        } catch (JsonException $error) {
            throw new RuntimeException('Worker target could not be encoded as JSON.', previous: $error);
        }
    }

    private function formatDate(DateTimeImmutable $date): string
    {
        return $date->format(DATE_ATOM);
    }

    private function quoteIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }
}
