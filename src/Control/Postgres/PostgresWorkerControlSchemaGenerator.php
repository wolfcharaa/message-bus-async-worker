<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control\Postgres;

use Fedor\MessageBus\Worker\Control\WorkerControlTableDefinition;

final class PostgresWorkerControlSchemaGenerator
{
    public function generate(WorkerControlTableDefinition $definition = new WorkerControlTableDefinition()): string
    {
        $commands = $this->quoteIdentifier($definition->commandsTable);
        $desiredStates = $this->quoteIdentifier($definition->desiredStatesTable);
        $workers = $this->quoteIdentifier($definition->workerInstancesTable);
        $children = $this->quoteIdentifier($definition->childInstancesTable);
        $deliveries = $this->quoteIdentifier($definition->commandDeliveriesTable);
        $acknowledgements = $this->quoteIdentifier($definition->acknowledgementsTable);
        $audit = $this->quoteIdentifier($definition->commandAuditTable);

        $commandsCreatedAtIndex = $this->index($definition->commandsTable, 'created_at_idx');
        $commandsExpiresAtIndex = $this->index($definition->commandsTable, 'expires_at_idx');
        $commandsInstanceIndex = $this->index($definition->commandsTable, 'target_instance_idx');
        $commandsGroupIndex = $this->index($definition->commandsTable, 'target_group_idx');
        $desiredActiveIndex = $this->index($definition->desiredStatesTable, 'active_idx');
        $desiredGroupIndex = $this->index($definition->desiredStatesTable, 'target_group_idx');
        $workersHeartbeatIndex = $this->index($definition->workerInstancesTable, 'heartbeat_idx');
        $workersStateIndex = $this->index($definition->workerInstancesTable, 'state_idx');
        $workersTargetIndex = $this->index($definition->workerInstancesTable, 'target_idx');
        $childrenParentIndex = $this->index($definition->childInstancesTable, 'parent_idx');
        $childrenStateIndex = $this->index($definition->childInstancesTable, 'state_idx');
        $childrenMessageIndex = $this->index($definition->childInstancesTable, 'queue_message_idx');
        $deliveriesCommandIndex = $this->index($definition->commandDeliveriesTable, 'command_idx');
        $deliveriesTargetIndex = $this->index($definition->commandDeliveriesTable, 'target_idx');
        $acknowledgementsActorIndex = $this->index($definition->acknowledgementsTable, 'actor_stage_uidx');
        $acknowledgementsCommandIndex = $this->index($definition->acknowledgementsTable, 'command_idx');
        $acknowledgementsWorkerIndex = $this->index($definition->acknowledgementsTable, 'worker_idx');
        $auditCommandIndex = $this->index($definition->commandAuditTable, 'command_idx');
        $auditActorIndex = $this->index($definition->commandAuditTable, 'actor_idx');

        return <<<SQL
CREATE TABLE IF NOT EXISTS {$commands} (
    id BIGSERIAL PRIMARY KEY,
    command_id TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    target_type TEXT NOT NULL DEFAULT 'legacy',
    target_actor_type TEXT NULL,
    target_actor_id TEXT NULL,
    target_parent_actor_id TEXT NULL,
    target_worker_id TEXT NULL,
    target_worker_name TEXT NULL,
    target_worker_instance_id TEXT NULL,
    target_worker_group TEXT NULL,
    target_transport TEXT NULL,
    target_queue TEXT NULL,
    target_flows JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_binding_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_binding_patterns JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_mode TEXT NULL,
    target_host TEXT NULL,
    target_all BOOLEAN NOT NULL DEFAULT FALSE,
    target_specificity INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    created_by TEXT NULL,
    source TEXT NOT NULL,
    initiator_actor_id TEXT NULL,
    initiator_actor_type TEXT NULL,
    initiator_display_name TEXT NULL,
    initiator_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    reason TEXT NULL,
    request_id TEXT NULL,
    correlation_id TEXT NULL,
    deadline_at TIMESTAMPTZ NULL,
    expires_at TIMESTAMPTZ NULL,
    idempotency_key TEXT NULL UNIQUE,
    override BOOLEAN NOT NULL DEFAULT FALSE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS {$commandsCreatedAtIndex}
    ON {$commands} (id ASC, created_at ASC);

CREATE INDEX IF NOT EXISTS {$commandsExpiresAtIndex}
    ON {$commands} (expires_at)
    WHERE expires_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS {$commandsInstanceIndex}
    ON {$commands} (target_worker_instance_id)
    WHERE target_worker_instance_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS {$commandsGroupIndex}
    ON {$commands} (target_worker_group)
    WHERE target_worker_group IS NOT NULL;

CREATE TABLE IF NOT EXISTS {$desiredStates} (
    id BIGSERIAL PRIMARY KEY,
    desired_state_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL,
    scope_type TEXT NULL,
    scope_id TEXT NULL,
    concurrency_override INTEGER NULL CHECK (concurrency_override IS NULL OR concurrency_override >= 0),
    drain_requested_at TIMESTAMPTZ NULL,
    stop_deadline_at TIMESTAMPTZ NULL,
    restart_deadline_at TIMESTAMPTZ NULL,
    runtime_overrides JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_command_id TEXT NULL,
    target_worker_id TEXT NULL,
    target_worker_name TEXT NULL,
    target_worker_instance_id TEXT NULL,
    target_worker_group TEXT NULL,
    target_transport TEXT NULL,
    target_queue TEXT NULL,
    target_flows JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_binding_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_binding_patterns JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_mode TEXT NULL,
    target_host TEXT NULL,
    target_all BOOLEAN NOT NULL DEFAULT FALSE,
    target_specificity INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    created_by TEXT NULL,
    source TEXT NOT NULL,
    reason TEXT NULL,
    request_id TEXT NULL,
    correlation_id TEXT NULL,
    override BOOLEAN NOT NULL DEFAULT FALSE,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS {$desiredActiveIndex}
    ON {$desiredStates} (active, target_specificity DESC, created_at DESC)
    WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS {$desiredGroupIndex}
    ON {$desiredStates} (target_worker_group)
    WHERE target_worker_group IS NOT NULL;

CREATE TABLE IF NOT EXISTS {$workers} (
    worker_instance_id TEXT PRIMARY KEY,
    actor_id TEXT NULL UNIQUE,
    actor_type TEXT NOT NULL DEFAULT 'main_worker',
    parent_actor_id TEXT NULL,
    display_name TEXT NULL,
    worker_id TEXT NULL,
    worker_name TEXT NOT NULL,
    worker_group TEXT NULL,
    host TEXT NOT NULL,
    pid INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    mode TEXT NOT NULL,
    transport TEXT NOT NULL,
    queue TEXT NOT NULL,
    flows JSONB NOT NULL DEFAULT '[]'::jsonb,
    binding_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    binding_patterns JSONB NOT NULL DEFAULT '[]'::jsonb,
    state TEXT NOT NULL,
    activity TEXT NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    children_count INTEGER NOT NULL DEFAULT 0,
    last_command_id TEXT NULL,
    last_command_at TIMESTAMPTZ NULL,
    stopped_at TIMESTAMPTZ NULL,
    failure_message TEXT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS {$workersHeartbeatIndex} ON {$workers} (heartbeat_at);
CREATE INDEX IF NOT EXISTS {$workersStateIndex} ON {$workers} (state, activity);
CREATE INDEX IF NOT EXISTS {$workersTargetIndex} ON {$workers} (worker_group, host, transport, queue, mode);

CREATE TABLE IF NOT EXISTS {$children} (
    child_instance_id TEXT PRIMARY KEY,
    actor_id TEXT NULL UNIQUE,
    actor_type TEXT NOT NULL DEFAULT 'child_worker',
    parent_actor_id TEXT NULL,
    parent_worker_instance_id TEXT NOT NULL REFERENCES {$workers} (worker_instance_id) ON DELETE CASCADE,
    pid INTEGER NOT NULL,
    state TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    queue_message_id TEXT NULL,
    message_id TEXT NULL,
    correlation_id TEXT NULL,
    binding_id TEXT NULL,
    finished_at TIMESTAMPTZ NULL,
    failure_message TEXT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS {$childrenParentIndex} ON {$children} (parent_worker_instance_id);
CREATE INDEX IF NOT EXISTS {$childrenStateIndex} ON {$children} (state, heartbeat_at);
CREATE INDEX IF NOT EXISTS {$childrenMessageIndex}
    ON {$children} (queue_message_id)
    WHERE queue_message_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS {$deliveries} (
    id BIGSERIAL PRIMARY KEY,
    delivery_id TEXT NOT NULL UNIQUE,
    command_id TEXT NOT NULL REFERENCES {$commands} (command_id) ON DELETE CASCADE,
    target_type TEXT NOT NULL,
    target_actor_type TEXT NULL,
    target_actor_id TEXT NULL,
    target_parent_actor_id TEXT NULL,
    target_worker_group TEXT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    seen_at TIMESTAMPTZ NULL,
    applying_at TIMESTAMPTZ NULL,
    applied_at TIMESTAMPTZ NULL,
    skipped_at TIMESTAMPTZ NULL,
    expired_at TIMESTAMPTZ NULL,
    failed_at TIMESTAMPTZ NULL,
    error_message TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (command_id, target_actor_type, target_actor_id)
);

CREATE INDEX IF NOT EXISTS {$deliveriesCommandIndex} ON {$deliveries} (command_id, status, updated_at);
CREATE INDEX IF NOT EXISTS {$deliveriesTargetIndex} ON {$deliveries} (target_type, target_actor_type, target_actor_id);

CREATE TABLE IF NOT EXISTS {$acknowledgements} (
    id BIGSERIAL PRIMARY KEY,
    acknowledgement_id TEXT NULL UNIQUE,
    command_id TEXT NOT NULL REFERENCES {$commands} (command_id) ON DELETE CASCADE,
    delivery_id TEXT NULL,
    worker_instance_id TEXT NOT NULL REFERENCES {$workers} (worker_instance_id) ON DELETE CASCADE,
    actor_type TEXT NULL,
    actor_id TEXT NULL,
    parent_actor_id TEXT NULL,
    stage TEXT NOT NULL DEFAULT 'applied',
    result TEXT NULL,
    state TEXT NOT NULL,
    acknowledged_at TIMESTAMPTZ NOT NULL,
    message TEXT NULL,
    error_class TEXT NULL,
    error_message TEXT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (command_id, worker_instance_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS {$acknowledgementsActorIndex}
    ON {$acknowledgements} (command_id, actor_type, actor_id, stage)
    WHERE actor_type IS NOT NULL AND actor_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS {$acknowledgementsCommandIndex}
    ON {$acknowledgements} (command_id, acknowledged_at);
CREATE INDEX IF NOT EXISTS {$acknowledgementsWorkerIndex}
    ON {$acknowledgements} (worker_instance_id, acknowledged_at);

CREATE TABLE IF NOT EXISTS {$audit} (
    id BIGSERIAL PRIMARY KEY,
    audit_id TEXT NULL UNIQUE,
    command_id TEXT NULL REFERENCES {$commands} (command_id) ON DELETE SET NULL,
    delivery_id TEXT NULL,
    actor_type TEXT NULL,
    actor_id TEXT NULL,
    parent_actor_id TEXT NULL,
    event TEXT NOT NULL,
    level TEXT NOT NULL DEFAULT 'info',
    message TEXT NULL,
    sanitized_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS {$auditCommandIndex} ON {$audit} (command_id, created_at);
CREATE INDEX IF NOT EXISTS {$auditActorIndex} ON {$audit} (actor_type, actor_id, created_at);
SQL;
    }

    private function index(string $table, string $suffix): string
    {
        return $this->quoteIdentifier($table . '_' . $suffix);
    }

    private function quoteIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }
}
