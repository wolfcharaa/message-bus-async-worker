<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Cli\Command;

use Closure;
use DateTimeImmutable;
use Fedor\MessageBus\Worker\Cli\BootstrapResolver;
use Fedor\MessageBus\Worker\Cli\InputOptionValue;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandReceipt;
use Fedor\MessageBus\Worker\Control\WorkerControlCommandType;
use Fedor\MessageBus\Worker\Control\WorkerControlRequest;
use Fedor\MessageBus\Worker\Control\WorkerControlRuntime;
use Fedor\MessageBus\Worker\Control\WorkerMode;
use Fedor\MessageBus\Worker\Control\WorkerTarget;
use Fedor\MessageBus\Worker\Runtime\WorkerRuntime;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

abstract class AbstractWorkerControlCommand extends Command
{
    public function __construct(
        string $name,
        private readonly BootstrapResolver $bootstrapResolver,
    ) {
        parent::__construct($name);
    }

    protected function addBootstrapOption(): void
    {
        $this->addOption('bootstrap', null, InputOption::VALUE_REQUIRED);
    }

    protected function addTargetOptions(): void
    {
        $this
            ->addOption('worker-id', null, InputOption::VALUE_REQUIRED)
            ->addOption('worker-name', null, InputOption::VALUE_REQUIRED)
            ->addOption('worker-instance-id', null, InputOption::VALUE_REQUIRED)
            ->addOption('group', null, InputOption::VALUE_REQUIRED)
            ->addOption('transport', null, InputOption::VALUE_REQUIRED)
            ->addOption('queue', null, InputOption::VALUE_REQUIRED)
            ->addOption('flow', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY)
            ->addOption('binding-id', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY)
            ->addOption('binding-pattern', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY)
            ->addOption('mode', null, InputOption::VALUE_REQUIRED)
            ->addOption('host', null, InputOption::VALUE_REQUIRED)
            ->addOption('all', null, InputOption::VALUE_NONE);
    }

    protected function addAuditOptions(): void
    {
        $this
            ->addOption('created-by', null, InputOption::VALUE_REQUIRED)
            ->addOption('source', null, InputOption::VALUE_REQUIRED, default: 'cli')
            ->addOption('reason', null, InputOption::VALUE_REQUIRED)
            ->addOption('request-id', null, InputOption::VALUE_REQUIRED)
            ->addOption('correlation-id', null, InputOption::VALUE_REQUIRED)
            ->addOption('expires-at', null, InputOption::VALUE_REQUIRED)
            ->addOption('expires-in', null, InputOption::VALUE_REQUIRED)
            ->addOption('idempotency-key', null, InputOption::VALUE_REQUIRED)
            ->addOption('override', null, InputOption::VALUE_NONE);
    }

    /**
     * @template TResult
     * @param Closure(WorkerControlRuntime): TResult $operation
     * @return TResult
     */
    protected function withControlRuntime(InputInterface $input, Closure $operation): mixed
    {
        return $this->bootstrapResolver
            ->run(
                InputOptionValue::nullableString($input, 'bootstrap'),
                static fn(WorkerRuntime $runtime): mixed => $operation($runtime->requireControl()),
            );
    }

    protected function target(InputInterface $input, bool $defaultAll = false): WorkerTarget
    {
        if ($defaultAll && !$this->hasTargetFilters($input) && !InputOptionValue::boolean($input, 'all')) {
            return WorkerTarget::all();
        }

        $mode = InputOptionValue::nullableString($input, 'mode');

        return new WorkerTarget(
            workerId: InputOptionValue::nullableString($input, 'worker-id'),
            workerName: InputOptionValue::nullableString($input, 'worker-name'),
            workerInstanceId: InputOptionValue::nullableString($input, 'worker-instance-id'),
            workerGroup: InputOptionValue::nullableString($input, 'group'),
            transport: InputOptionValue::nullableString($input, 'transport'),
            queue: InputOptionValue::nullableString($input, 'queue'),
            flows: InputOptionValue::stringList($input, 'flow'),
            bindingIds: InputOptionValue::stringList($input, 'binding-id'),
            bindingPatterns: InputOptionValue::stringList($input, 'binding-pattern'),
            mode: $mode !== null ? WorkerMode::from($mode) : null,
            host: InputOptionValue::nullableString($input, 'host'),
            all: InputOptionValue::boolean($input, 'all'),
        );
    }

    protected function request(InputInterface $input): WorkerControlRequest
    {
        return new WorkerControlRequest(
            createdBy: InputOptionValue::nullableString($input, 'created-by'),
            source: InputOptionValue::string($input, 'source'),
            reason: InputOptionValue::nullableString($input, 'reason'),
            requestId: InputOptionValue::nullableString($input, 'request-id'),
            correlationId: InputOptionValue::nullableString($input, 'correlation-id'),
            expiresAt: $this->expiresAt($input),
            idempotencyKey: InputOptionValue::nullableString($input, 'idempotency-key'),
            override: InputOptionValue::boolean($input, 'override'),
        );
    }

    protected function writeReceipt(OutputInterface $output, WorkerControlCommandReceipt $receipt): int
    {
        $output->writeln(sprintf(
            'command=%s type=%s duplicate=%s',
            $receipt->commandId,
            $receipt->type->value,
            $receipt->duplicate ? 'yes' : 'no',
        ));

        return self::SUCCESS;
    }

    protected function dispatchControl(
        WorkerControlRuntime $runtime,
        WorkerControlCommandType $type,
        WorkerTarget $target,
        WorkerControlRequest $request,
    ): WorkerControlCommandReceipt {
        return match ($type) {
            WorkerControlCommandType::Pause => $runtime->controlService->pause($target, $request),
            WorkerControlCommandType::Resume => $runtime->controlService->resume($target, $request),
            WorkerControlCommandType::Drain => $runtime->controlService->drain($target, $request),
            WorkerControlCommandType::Stop => $runtime->controlService->stop($target, $request),
            WorkerControlCommandType::Kill => $runtime->controlService->kill($target, $request),
            WorkerControlCommandType::Restart => $runtime->controlService->restart($target, $request),
        };
    }

    private function hasTargetFilters(InputInterface $input): bool
    {
        return InputOptionValue::nullableString($input, 'worker-id') !== null
            || InputOptionValue::nullableString($input, 'worker-name') !== null
            || InputOptionValue::nullableString($input, 'worker-instance-id') !== null
            || InputOptionValue::nullableString($input, 'group') !== null
            || InputOptionValue::nullableString($input, 'transport') !== null
            || InputOptionValue::nullableString($input, 'queue') !== null
            || InputOptionValue::stringList($input, 'flow') !== []
            || InputOptionValue::stringList($input, 'binding-id') !== []
            || InputOptionValue::stringList($input, 'binding-pattern') !== []
            || InputOptionValue::nullableString($input, 'mode') !== null
            || InputOptionValue::nullableString($input, 'host') !== null;
    }

    private function expiresAt(InputInterface $input): ?DateTimeImmutable
    {
        $expiresAt = InputOptionValue::nullableString($input, 'expires-at');
        if ($expiresAt !== null) {
            return new DateTimeImmutable($expiresAt);
        }

        $expiresIn = InputOptionValue::nullableInteger($input, 'expires-in');

        return $expiresIn !== null
            ? (new DateTimeImmutable())->modify(sprintf('+%d seconds', $expiresIn))
            : null;
    }
}
