<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Cli\Command;

use Fedor\MessageBus\Worker\Cli\BootstrapResolver;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandType;
use Fedor\MessageBus\Worker\Control\WorkerControlRuntime;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

final class WorkerControlActionCommand extends AbstractWorkerControlCommand
{
    public function __construct(
        private readonly WorkerControlCommandType $type,
        string $name,
        BootstrapResolver $bootstrapResolver = new BootstrapResolver(),
    ) {
        parent::__construct($name, $bootstrapResolver);
    }

    protected function configure(): void
    {
        $this->setDescription('Send worker ' . $this->type->value . ' command.');
        $this->addBootstrapOption();
        $this->addTargetOptions();
        $this->addAuditOptions();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $target = $this->target($input);
        $request = $this->request($input);

        return $this->withControlRuntime(
            $input,
            fn(WorkerControlRuntime $runtime): int => $this->writeReceipt(
                $output,
                $this->dispatchControl($runtime, $this->type, $target, $request),
            ),
        );
    }
}
