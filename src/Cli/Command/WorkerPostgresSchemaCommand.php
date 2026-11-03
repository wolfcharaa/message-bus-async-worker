<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Cli\Command;

use Fedor\MessageBus\Worker\Cli\InputOptionValue;
use Fedor\MessageBus\Worker\Control\Postgres\PostgresWorkerControlSchemaGenerator;
use RuntimeException;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

final class WorkerPostgresSchemaCommand extends Command
{
    public function __construct()
    {
        parent::__construct('worker:schema:postgres');
    }

    protected function configure(): void
    {
        $this
            ->setDescription('Generate PostgreSQL schema for worker control tables.')
            ->addOption('output', null, InputOption::VALUE_REQUIRED);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $sql = (new PostgresWorkerControlSchemaGenerator())->generate();
        $outputPath = InputOptionValue::nullableString($input, 'output');

        if ($outputPath === null) {
            $output->writeln($sql);

            return self::SUCCESS;
        }

        if (file_put_contents($outputPath, $sql . PHP_EOL) === false) {
            throw new RuntimeException(sprintf('Unable to write worker schema to "%s".', $outputPath));
        }

        return self::SUCCESS;
    }
}
