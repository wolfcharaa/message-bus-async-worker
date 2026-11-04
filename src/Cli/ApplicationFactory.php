<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Cli;

use Fedor\MessageBus\Worker\Cli\Command\WorkerControlActionCommand;
use Fedor\MessageBus\Worker\Cli\Command\WorkerControlCommand;
use Fedor\MessageBus\Worker\Cli\Command\WorkerPostgresSchemaCommand;
use Fedor\MessageBus\Worker\Cli\Command\WorkerRunCommand;
use Fedor\MessageBus\Worker\Cli\Command\WorkerStatusCommand;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandType;
use Symfony\Component\Console\Application;

final class ApplicationFactory
{
    public static function create(?BootstrapResolver $bootstrapResolver = null): Application
    {
        $bootstrapResolver ??= new BootstrapResolver();
        $application = new Application('message-bus-async-worker');
        $application->addCommand(new WorkerRunCommand($bootstrapResolver));
        $application->addCommand(new WorkerControlCommand($bootstrapResolver));
        $application->addCommand(new WorkerControlActionCommand(
            WorkerControlCommandType::Pause,
            'worker:pause',
            $bootstrapResolver,
        ));
        $application->addCommand(new WorkerControlActionCommand(
            WorkerControlCommandType::Resume,
            'worker:resume',
            $bootstrapResolver,
        ));
        $application->addCommand(new WorkerControlActionCommand(
            WorkerControlCommandType::Drain,
            'worker:drain',
            $bootstrapResolver,
        ));
        $application->addCommand(new WorkerControlActionCommand(
            WorkerControlCommandType::Stop,
            'worker:stop',
            $bootstrapResolver,
        ));
        $application->addCommand(new WorkerControlActionCommand(
            WorkerControlCommandType::Kill,
            'worker:kill',
            $bootstrapResolver,
        ));
        $application->addCommand(new WorkerControlActionCommand(
            WorkerControlCommandType::Restart,
            'worker:restart',
            $bootstrapResolver,
        ));
        $application->addCommand(new WorkerStatusCommand($bootstrapResolver));
        $application->addCommand(new WorkerPostgresSchemaCommand());

        return $application;
    }
}
