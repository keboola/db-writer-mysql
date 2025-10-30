<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\Component\UserException;
use Keboola\DbWriterAdapter\PDO\PdoConnection;
use Pdo;
use PDOStatement;
use Throwable;

class MySQLConnection extends PdoConnection
{

    private string $charset = 'utf8mb4';

    public function getCharset(): string
    {
        return $this->charset;
    }

    public function setCharset(string $charset): void
    {
        $this->charset = $charset;
    }

    protected function connect(): void
    {
        try {
            parent::connect();
        } catch (Throwable $e) {
            $this->handleException($e);
        }
    }

    protected function doQuery(string $queryType, string $query): ?array
    {
        switch ($queryType) {
            case self::QUERY_TYPE_EXEC:
                $this->pdo->prepare($query)->execute();
                $this->logMySQLWarnings();
                return null;
            case self::QUERY_TYPE_FETCH_ALL:
                $stmt = $this->pdo->query($query);
                if (!$stmt instanceof PDOStatement) {
                    return [];
                }
                $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
                $stmt->closeCursor();
                return $result;
            default:
                throw new UserException(sprintf('Unknown query type "%s".', $queryType));
        }
    }

    protected function handleException(Throwable $e): void
    {
        $checkCnMismatch = function (Throwable $exception): void {
            if (str_contains($exception->getMessage(), 'did not match expected CN')) {
                throw new UserException($exception->getMessage());
            }
        };
        $checkCnMismatch($e);
        $previous = $e->getPrevious();
        if ($previous !== null) {
            $checkCnMismatch($previous);
        }

        // SQLSTATE[HY000] is a main general message
        // and additional informations are in the previous exception, so throw previous
        if (str_starts_with($e->getMessage(), 'SQLSTATE[HY000]') && $e->getPrevious() !== null) {
            throw $e->getPrevious();
        }
        throw $e;
    }

    private function logMySQLWarnings(): void
    {
        /** @var array{Message: string}[] $warnings */
        $warnings = $this->fetchAll('SHOW WARNINGS', 1);
        foreach ($warnings as $warning) {
            $this->logger->warning($warning['Message']);
        }
    }
}
