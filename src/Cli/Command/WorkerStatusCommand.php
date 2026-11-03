<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Cli\Command;

use Fedor\MessageBus\Worker\Cli\BootstrapResolver;
use Fedor\MessageBus\Worker\Cli\InputOptionValue;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

final class WorkerStatusCommand extends AbstractWorkerControlCommand
{
    public function __construct(BootstrapResolver $bootstrapResolver = new BootstrapResolver())
    {
        parent::__construct('worker:status', $bootstrapResolver);
    }

    protected function configure(): void
    {
        $this
            ->setDescription('Show registered worker status.')
            ->addOption('children', null, InputOption::VALUE_NONE);
        $this->addBootstrapOption();
        $this->addTargetOptions();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $runtime = $this->controlRuntime($input);
        $workers = $runtime->statusService->listWorkers($this->target($input, defaultAll: true));

        $table = new Table($output);
        $table->setHeaders([
            'instance', 'name', 'group', 'host', 'pid', 'mode', 'state', 'activity', 'children', 'queue', 'heartbeat',
        ]);
        foreach ($workers as $worker) {
            $table->addRow([
                $worker->identity->workerInstanceId,
                $worker->identity->workerName,
                $worker->identity->workerGroup ?? '',
                $worker->identity->host,
                (string) $worker->identity->pid,
                $worker->identity->mode->value,
                $worker->state->value,
                $worker->activity->value,
                (string) $worker->childrenCount,
                $worker->identity->transport . '/' . $worker->identity->queue,
                $worker->heartbeatAt->format(DATE_ATOM),
            ]);
        }
        $table->render();

        if (InputOptionValue::boolean($input, 'children')) {
            foreach ($workers as $worker) {
                $children = $runtime->statusService->listChildren($worker->identity->workerInstanceId);
                if ($children === []) {
                    continue;
                }

                $output->writeln('');
                $output->writeln('children=' . $worker->identity->workerInstanceId);
                $childTable = new Table($output);
                $childTable->setHeaders([
                    'child', 'pid', 'state', 'queueMessageId', 'messageId', 'correlationId', 'bindingId', 'heartbeat',
                ]);
                foreach ($children as $child) {
                    $childTable->addRow([
                        $child->childInstanceId,
                        (string) $child->pid,
                        $child->state->value,
                        $child->queueMessageId ?? '',
                        $child->messageId ?? '',
                        $child->correlationId ?? '',
                        $child->bindingId ?? '',
                        $child->heartbeatAt->format(DATE_ATOM),
                    ]);
                }
                $childTable->render();
            }
        }

        return self::SUCCESS;
    }
}
