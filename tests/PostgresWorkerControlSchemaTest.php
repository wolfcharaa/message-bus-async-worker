<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Tests;

use Fedor\MessageBus\Worker\Control\Postgres\PostgresWorkerControlSchemaGenerator;
use Fedor\MessageBus\Worker\Control\WorkerControlTableDefinition;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class PostgresWorkerControlSchemaTest extends TestCase
{
    public function testSchemaOwnsTheCompleteV6WorkerControlTableSet(): void
    {
        $definition = new WorkerControlTableDefinition(
            commandsTable: 'control_commands',
            desiredStatesTable: 'control_states',
            workerInstancesTable: 'control_workers',
            childInstancesTable: 'control_children',
            acknowledgementsTable: 'control_acknowledgements',
            commandDeliveriesTable: 'control_deliveries',
            commandAuditTable: 'control_audit',
        );

        $schema = (new PostgresWorkerControlSchemaGenerator())->generate($definition);

        foreach ([
            'control_commands',
            'control_states',
            'control_workers',
            'control_children',
            'control_acknowledgements',
            'control_deliveries',
            'control_audit',
        ] as $table) {
            self::assertStringContainsString('CREATE TABLE IF NOT EXISTS "' . $table . '"', $schema);
        }
        self::assertStringContainsString('idempotency_key TEXT NULL UNIQUE', $schema);
        self::assertStringContainsString('target_specificity INTEGER NOT NULL', $schema);
        self::assertStringContainsString('UNIQUE (command_id, worker_instance_id)', $schema);
    }

    public function testTableDefinitionRejectsUnsafeIdentifiers(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new WorkerControlTableDefinition(commandsTable: 'public.worker_commands');
    }
}
