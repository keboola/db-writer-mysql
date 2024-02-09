<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriterAdapter\PDO\PdoWriteAdapter;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Exception\PropertyNotSetException;

/**
 * @property MySQLQueryBuilder $queryBuilder
 * @property MySQLConnection $connection
 */
class MySQLWriteAdapter extends PdoWriteAdapter
{
    /**
     * @throws UserException
     * @throws PropertyNotSetException
     */
    public function upsert(ExportConfig $exportConfig, string $stageTableName): void
    {
        $this->logger->info(sprintf(
            'Upserting data into table "%s"',
            $exportConfig->getDbName(),
        ));

        if ($exportConfig->hasPrimaryKey()) {
            $this->checkIfPrimaryKeysExist($exportConfig);
            $this->connection->exec(
                $this->queryBuilder->upsertWithPrimaryKeys($this->connection, $exportConfig, $stageTableName),
            );
        } else {
            $this->connection->exec(
                $this->queryBuilder->upsertWithoutPrimaryKeys($this->connection, $exportConfig, $stageTableName),
            );
        }
        $this->logger->info(sprintf(
            'Upserted data into table "%s"',
            $exportConfig->getDbName(),
        ));
    }

    /**
     * @throws UserException|PropertyNotSetException
     */
    private function checkIfPrimaryKeysExist(ExportConfig $exportConfig): void
    {
        $dbPrimaryKeys = array_map(
            fn(array $row) => $row['Column_name'],
            $this->connection->fetchAll(
                $this->queryBuilder->getPrimaryKeysQuery($this->connection, $exportConfig->getDbName()),
                5,
            ),
        );
        $configKeys = $exportConfig->getPrimaryKey();
        sort($dbPrimaryKeys);
        sort($configKeys);

        if ($dbPrimaryKeys !== $configKeys) {
            throw new UserException(sprintf(
                'Primary key(s) in configuration does NOT match with keys in DB table.' . PHP_EOL
                . 'Keys in configuration: %s' . PHP_EOL
                . 'Keys in DB table: %s',
                implode(',', $configKeys),
                implode(',', $dbPrimaryKeys),
            ));
        }
    }
}
