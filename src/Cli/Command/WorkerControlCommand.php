<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Cli\Command;

use Fedor\MessageBus\Worker\Cli\BootstrapResolver;
use Fedor\MessageBus\Worker\Cli\InputOptionValue;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandType;
use Fedor\MessageBus\Worker\Control\WorkerControlRuntime;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

final class WorkerControlCommand extends AbstractWorkerControlCommand
{
    public function __construct(BootstrapResolver $bootstrapResolver = new BootstrapResolver())
    {
        parent::__construct('worker:control', $bootstrapResolver);
    }

    protected function configure(): void
    {
        $this
            ->setDescription('Send a worker control command.')
            ->addOption('command', null, InputOption::VALUE_REQUIRED);
        $this->addBootstrapOption();
        $this->addTargetOptions();
        $this->addAuditOptions();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $type = WorkerControlCommandType::from(InputOptionValue::string($input, 'command'));
        $target = $this->target($input);
        $request = $this->request($input);

        return $this->withControlRuntime(
            $input,
            fn(WorkerControlRuntime $runtime): int => $this->writeReceipt(
                $output,
                $this->dispatchControl($runtime, $type, $target, $request),
            ),
        );
    }
}
