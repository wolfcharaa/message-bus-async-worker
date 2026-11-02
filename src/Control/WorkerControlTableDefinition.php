<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Control;

use InvalidArgumentException;

final readonly class WorkerControlTableDefinition
{
    public function __construct(
        public string $commandsTable = 'message_bus__worker_control_commands',
        public string $desiredStatesTable = 'message_bus__worker_desired_state',
        public string $workerInstancesTable = 'message_bus__worker_instances',
        public string $childInstancesTable = 'message_bus__worker_child_instances',
        public string $acknowledgementsTable = 'message_bus__worker_control_command_acknowledgements',
        public string $commandDeliveriesTable = 'message_bus__worker_control_command_deliveries',
        public string $commandAuditTable = 'message_bus__worker_control_command_audit',
    ) {
        foreach ([
            $this->commandsTable,
            $this->desiredStatesTable,
            $this->workerInstancesTable,
            $this->childInstancesTable,
            $this->acknowledgementsTable,
            $this->commandDeliveriesTable,
            $this->commandAuditTable,
        ] as $table) {
            if (preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $table) !== 1) {
                throw new InvalidArgumentException(
                    'Worker control table names must be PostgreSQL identifiers without a schema prefix.',
                );
            }
        }
    }
}
