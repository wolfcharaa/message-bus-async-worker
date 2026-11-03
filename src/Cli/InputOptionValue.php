<?php

declare(strict_types=1);

namespace Fedor\MessageBus\Worker\Cli;

use InvalidArgumentException;
use Symfony\Component\Console\Input\InputInterface;

final class InputOptionValue
{
    public static function string(InputInterface $input, string $name): string
    {
        $value = $input->getOption($name);
        if (!is_string($value)) {
            throw new InvalidArgumentException(sprintf('Option --%s must be a string.', $name));
        }

        return $value;
    }

    public static function nullableString(InputInterface $input, string $name): ?string
    {
        $value = $input->getOption($name);
        if ($value !== null && !is_string($value)) {
            throw new InvalidArgumentException(sprintf('Option --%s must be a string.', $name));
        }

        return $value;
    }

    public static function integer(InputInterface $input, string $name): int
    {
        $value = $input->getOption($name);
        if (is_int($value)) {
            return $value;
        }
        if (is_string($value) && preg_match('/^-?\d+$/', $value) === 1) {
            return (int) $value;
        }

        throw new InvalidArgumentException(sprintf('Option --%s must be an integer.', $name));
    }

    public static function nullableInteger(InputInterface $input, string $name): ?int
    {
        return $input->getOption($name) === null ? null : self::integer($input, $name);
    }

    /** @return list<string> */
    public static function stringList(InputInterface $input, string $name): array
    {
        $value = $input->getOption($name);
        if (!is_array($value) || !array_is_list($value)) {
            throw new InvalidArgumentException(sprintf('Option --%s must be a list of strings.', $name));
        }
        foreach ($value as $item) {
            if (!is_string($item)) {
                throw new InvalidArgumentException(sprintf('Option --%s must be a list of strings.', $name));
            }
        }

        return $value;
    }

    public static function boolean(InputInterface $input, string $name): bool
    {
        $value = $input->getOption($name);
        if (!is_bool($value)) {
            throw new InvalidArgumentException(sprintf('Option --%s must be a boolean.', $name));
        }

        return $value;
    }
}
