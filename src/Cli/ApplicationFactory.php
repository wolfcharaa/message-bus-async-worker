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
        $application->add(new WorkerRunCommand($bootstrapResolver));
        $application->add(new WorkerControlCommand($bootstrapResolver));
        $application->add(new WorkerControlActionCommand(
            WorkerControlCommandType::Pause,
            'worker:pause',
            $bootstrapResolver,
        ));
        $application->add(new WorkerControlActionCommand(
            WorkerControlCommandType::Resume,
            'worker:resume',
            $bootstrapResolver,
        ));
        $application->add(new WorkerControlActionCommand(
            WorkerControlCommandType::Drain,
            'worker:drain',
            $bootstrapResolver,
        ));
        $application->add(new WorkerControlActionCommand(
            WorkerControlCommandType::Stop,
            'worker:stop',
            $bootstrapResolver,
        ));
        $application->add(new WorkerControlActionCommand(
            WorkerControlCommandType::Kill,
            'worker:kill',
            $bootstrapResolver,
        ));
        $application->add(new WorkerControlActionCommand(
            WorkerControlCommandType::Restart,
            'worker:restart',
            $bootstrapResolver,
        ));
        $application->add(new WorkerStatusCommand($bootstrapResolver));
        $application->add(new WorkerPostgresSchemaCommand());

        return $application;
    }
}
