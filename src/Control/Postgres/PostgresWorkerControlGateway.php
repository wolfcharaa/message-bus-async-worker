<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control\Postgres;

use Amp\Cancellation;
use Amp\Postgres\PostgresLink;
use DateTimeImmutable;
use Fedor\MessageBus\Worker\Clock\UtcSystemClock;
use Fedor\MessageBus\Worker\Control\WorkerActivityState;
use Fedor\MessageBus\Worker\Control\WorkerChildInstance;
use Fedor\MessageBus\Worker\Control\WorkerChildState;
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
use Fedor\MessageBus\Worker\Control\WorkerInstance;
use Fedor\MessageBus\Worker\Control\WorkerLifecycleState;
use Fedor\MessageBus\Worker\Control\WorkerRegistryInterface;
use Fedor\MessageBus\Worker\Control\WorkerStatusRepositoryInterface;
use Fedor\MessageBus\Worker\Control\WorkerTarget;
use JsonException;
use Psr\Clock\ClockInterface;
use RuntimeException;

final class PostgresWorkerControlGateway implements
    WorkerControlCommandRepositoryInterface,
    WorkerDesiredStateRepositoryInterface,
    WorkerControlInboxInterface,
    WorkerRegistryInterface,
    WorkerStatusRepositoryInterface
{
    private readonly string $commandsTable;
    private readonly string $desiredStatesTable;
    private readonly string $acknowledgementsTable;
    private readonly string $workerInstancesTable;
    private readonly string $childInstancesTable;
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
        $this->workerInstancesTable = $this->quoteIdentifier($definition->workerInstancesTable);
        $this->childInstancesTable = $this->quoteIdentifier($definition->childInstancesTable);
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

    #[\Override]
    public function registerWorker(WorkerInstance $worker, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        $identity = $worker->identity;
        $this->postgres->execute(
            'INSERT INTO ' . $this->workerInstancesTable . ' (
                worker_instance_id, worker_id, worker_name, worker_group, host, pid, started_at,
                mode, transport, queue, flows, binding_ids, binding_patterns,
                state, activity, heartbeat_at, children_count,
                last_command_id, last_command_at, stopped_at, failure_message, updated_at
            ) VALUES (
                :worker_instance_id, :worker_id, :worker_name, :worker_group, :host, :pid, :started_at,
                :mode, :transport, :queue, :flows, :binding_ids, :binding_patterns,
                :state, :activity, :heartbeat_at, :children_count,
                :last_command_id, :last_command_at, :stopped_at, :failure_message, :updated_at
            )
            ON CONFLICT (worker_instance_id) DO UPDATE SET
                worker_id = EXCLUDED.worker_id,
                worker_name = EXCLUDED.worker_name,
                worker_group = EXCLUDED.worker_group,
                host = EXCLUDED.host,
                pid = EXCLUDED.pid,
                started_at = EXCLUDED.started_at,
                mode = EXCLUDED.mode,
                transport = EXCLUDED.transport,
                queue = EXCLUDED.queue,
                flows = EXCLUDED.flows,
                binding_ids = EXCLUDED.binding_ids,
                binding_patterns = EXCLUDED.binding_patterns,
                state = EXCLUDED.state,
                activity = EXCLUDED.activity,
                heartbeat_at = EXCLUDED.heartbeat_at,
                children_count = EXCLUDED.children_count,
                last_command_id = EXCLUDED.last_command_id,
                last_command_at = EXCLUDED.last_command_at,
                stopped_at = EXCLUDED.stopped_at,
                failure_message = EXCLUDED.failure_message,
                updated_at = EXCLUDED.updated_at',
            [
                'worker_instance_id' => $identity->workerInstanceId,
                'worker_id' => $identity->workerId,
                'worker_name' => $identity->workerName,
                'worker_group' => $identity->workerGroup,
                'host' => $identity->host,
                'pid' => $identity->pid,
                'started_at' => $this->formatDate($identity->startedAt),
                'mode' => $identity->mode->value,
                'transport' => $identity->transport,
                'queue' => $identity->queue,
                'flows' => $this->json($identity->flows),
                'binding_ids' => $this->json($identity->bindingIds),
                'binding_patterns' => $this->json($identity->bindingPatterns),
                'state' => $worker->state->value,
                'activity' => $worker->activity->value,
                'heartbeat_at' => $this->formatDate($worker->heartbeatAt),
                'children_count' => $worker->childrenCount,
                'last_command_id' => $worker->lastCommandId,
                'last_command_at' => $this->nullableDate($worker->lastCommandAt),
                'stopped_at' => $this->nullableDate($worker->stoppedAt),
                'failure_message' => $worker->failureMessage,
                'updated_at' => $this->formatDate($worker->heartbeatAt),
            ],
        );
    }

    #[\Override]
    public function heartbeatWorker(
        string $workerInstanceId,
        WorkerLifecycleState $state,
        WorkerActivityState $activity,
        DateTimeImmutable $heartbeatAt,
        Cancellation $cancellation,
        int $childrenCount = 0,
        ?string $lastCommandId = null,
    ): void {
        $cancellation->throwIfRequested();
        $this->postgres->execute(
            'UPDATE ' . $this->workerInstancesTable . '
             SET state = :state,
                 activity = :activity,
                 heartbeat_at = :heartbeat_at,
                 children_count = :children_count,
                 last_command_id = COALESCE(:last_command_id, last_command_id),
                 last_command_at = CASE
                     WHEN CAST(:last_command_id AS TEXT) IS NULL THEN last_command_at
                     ELSE :heartbeat_at
                 END,
                 updated_at = :heartbeat_at
             WHERE worker_instance_id = :worker_instance_id',
            [
                'worker_instance_id' => $workerInstanceId,
                'state' => $state->value,
                'activity' => $activity->value,
                'heartbeat_at' => $this->formatDate($heartbeatAt),
                'children_count' => $childrenCount,
                'last_command_id' => $lastCommandId,
            ],
        );
    }

    #[\Override]
    public function stopWorker(
        string $workerInstanceId,
        WorkerLifecycleState $state,
        DateTimeImmutable $stoppedAt,
        Cancellation $cancellation,
        ?string $failureMessage = null,
    ): void {
        $cancellation->throwIfRequested();
        $this->postgres->execute(
            'UPDATE ' . $this->workerInstancesTable . '
             SET state = :state,
                 activity = :activity,
                 heartbeat_at = :stopped_at,
                 stopped_at = :stopped_at,
                 failure_message = :failure_message,
                 updated_at = :stopped_at
             WHERE worker_instance_id = :worker_instance_id',
            [
                'worker_instance_id' => $workerInstanceId,
                'state' => $state->value,
                'activity' => WorkerActivityState::Idle->value,
                'stopped_at' => $this->formatDate($stoppedAt),
                'failure_message' => $failureMessage,
            ],
        );
    }

    #[\Override]
    public function registerChild(WorkerChildInstance $child, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        $this->postgres->execute(
            'INSERT INTO ' . $this->childInstancesTable . ' (
                child_instance_id, parent_worker_instance_id, pid, state, started_at, heartbeat_at,
                queue_message_id, message_id, correlation_id, binding_id,
                finished_at, failure_message, updated_at
            ) VALUES (
                :child_instance_id, :parent_worker_instance_id, :pid, :state, :started_at, :heartbeat_at,
                :queue_message_id, :message_id, :correlation_id, :binding_id,
                :finished_at, :failure_message, :updated_at
            )
            ON CONFLICT (child_instance_id) DO UPDATE SET
                parent_worker_instance_id = EXCLUDED.parent_worker_instance_id,
                pid = EXCLUDED.pid,
                state = EXCLUDED.state,
                started_at = EXCLUDED.started_at,
                heartbeat_at = EXCLUDED.heartbeat_at,
                queue_message_id = EXCLUDED.queue_message_id,
                message_id = EXCLUDED.message_id,
                correlation_id = EXCLUDED.correlation_id,
                binding_id = EXCLUDED.binding_id,
                finished_at = EXCLUDED.finished_at,
                failure_message = EXCLUDED.failure_message,
                updated_at = EXCLUDED.updated_at',
            [
                'child_instance_id' => $child->childInstanceId,
                'parent_worker_instance_id' => $child->parentWorkerInstanceId,
                'pid' => $child->pid,
                'state' => $child->state->value,
                'started_at' => $this->formatDate($child->startedAt),
                'heartbeat_at' => $this->formatDate($child->heartbeatAt),
                'queue_message_id' => $child->queueMessageId,
                'message_id' => $child->messageId,
                'correlation_id' => $child->correlationId,
                'binding_id' => $child->bindingId,
                'finished_at' => $this->nullableDate($child->finishedAt),
                'failure_message' => $child->failureMessage,
                'updated_at' => $this->formatDate($child->heartbeatAt),
            ],
        );
    }

    #[\Override]
    public function heartbeatChild(
        string $childInstanceId,
        WorkerChildState $state,
        DateTimeImmutable $heartbeatAt,
        Cancellation $cancellation,
    ): void {
        $cancellation->throwIfRequested();
        $this->postgres->execute(
            'UPDATE ' . $this->childInstancesTable . '
             SET state = :state,
                 heartbeat_at = :heartbeat_at,
                 updated_at = :heartbeat_at
             WHERE child_instance_id = :child_instance_id',
            [
                'child_instance_id' => $childInstanceId,
                'state' => $state->value,
                'heartbeat_at' => $this->formatDate($heartbeatAt),
            ],
        );
    }

    #[\Override]
    public function finishChild(
        string $childInstanceId,
        WorkerChildState $state,
        DateTimeImmutable $finishedAt,
        Cancellation $cancellation,
        ?string $failureMessage = null,
    ): void {
        $cancellation->throwIfRequested();
        $this->postgres->execute(
            'UPDATE ' . $this->childInstancesTable . '
             SET state = :state,
                 heartbeat_at = :finished_at,
                 finished_at = :finished_at,
                 failure_message = :failure_message,
                 updated_at = :finished_at
             WHERE child_instance_id = :child_instance_id',
            [
                'child_instance_id' => $childInstanceId,
                'state' => $state->value,
                'finished_at' => $this->formatDate($finishedAt),
                'failure_message' => $failureMessage,
            ],
        );
    }

    #[\Override]
    public function getWorker(
        string $workerInstanceId,
        Cancellation $cancellation,
    ): ?WorkerInstance {
        $cancellation->throwIfRequested();
        $row = $this->postgres->execute(
            'SELECT * FROM ' . $this->workerInstancesTable . '
             WHERE worker_instance_id = :worker_instance_id',
            ['worker_instance_id' => $workerInstanceId],
        )->fetchRow();

        return $row === null ? null : $this->records->worker($row);
    }

    #[\Override]
    public function listWorkers(WorkerTarget $target, Cancellation $cancellation): array
    {
        $cancellation->throwIfRequested();
        $result = $this->postgres->query(
            'SELECT * FROM ' . $this->workerInstancesTable . ' ORDER BY started_at ASC',
        );
        $workers = [];
        foreach ($result as $row) {
            $cancellation->throwIfRequested();
            $worker = $this->records->worker($row);
            if ($target->matches($worker->identity)) {
                $workers[] = $worker;
            }
        }

        return $workers;
    }

    #[\Override]
    public function listChildren(string $workerInstanceId, Cancellation $cancellation): array
    {
        $cancellation->throwIfRequested();
        $result = $this->postgres->execute(
            'SELECT * FROM ' . $this->childInstancesTable . '
             WHERE parent_worker_instance_id = :parent_worker_instance_id
             ORDER BY started_at ASC',
            ['parent_worker_instance_id' => $workerInstanceId],
        );
        $children = [];
        foreach ($result as $row) {
            $cancellation->throwIfRequested();
            $children[] = $this->records->child($row);
        }

        return $children;
    }

    #[\Override]
    public function acknowledgementsForCommand(
        string $commandId,
        Cancellation $cancellation,
    ): array {
        $cancellation->throwIfRequested();
        $result = $this->postgres->execute(
            'SELECT * FROM ' . $this->acknowledgementsTable . '
             WHERE command_id = :command_id
             ORDER BY acknowledged_at ASC',
            ['command_id' => $commandId],
        );
        $acknowledgements = [];
        foreach ($result as $row) {
            $cancellation->throwIfRequested();
            $acknowledgements[] = $this->records->acknowledgement($row);
        }

        return $acknowledgements;
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

    private function nullableDate(?DateTimeImmutable $date): ?string
    {
        return $date?->format(DATE_ATOM);
    }

    private function quoteIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }
}
