<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriterAdapter\DescriptionWriteAdapter;
use Keboola\DbWriterAdapter\PDO\PdoWriteAdapter;
use Keboola\DbWriterAdapter\ValueObject\TableDescriptions;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Exception\PropertyNotSetException;

/**
 * @property MySQLQueryBuilder $queryBuilder
 * @property MySQLConnection $connection
 */
class MySQLWriteAdapter extends PdoWriteAdapter implements DescriptionWriteAdapter
{
    private const TABLE_COMMENT_MAX_LENGTH = 2048;

    private const COLUMN_COMMENT_MAX_LENGTH = 1024;

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

    public function validateTable(string $tableName, array $items): void
    {
        // Don't validate table column types for MySQL - for BC reasons
    }

    public function drop(string $tableName): void
    {
        if (!$this->tableExists($tableName)) {
            return;
        }
        parent::drop($tableName);
    }

    public function applyDescriptions(string $tableName, TableDescriptions $descriptions): void
    {
        $tableComment = $descriptions->getTable();
        if ($tableComment !== null && $tableComment !== '') {
            $this->logger->info(sprintf('Setting comment on table "%s"', $tableName));
            $this->connection->exec($this->queryBuilder->tableCommentQueryStatement(
                $this->connection,
                $tableName,
                $this->truncateComment(
                    $tableComment,
                    self::TABLE_COMMENT_MAX_LENGTH,
                    sprintf('table "%s"', $tableName),
                ),
            ));
        }

        $columnComments = $descriptions->getColumns();
        if ($columnComments === []) {
            return;
        }

        $definitions = [];
        foreach ($this->fetchColumnDefinitions($tableName) as $definition) {
            // MySQL column names are case-insensitive.
            $definitions[mb_strtolower($definition['COLUMN_NAME'])] = $definition;
        }
        $isMariaDb = $this->isMariaDb();

        $columns = [];
        foreach ($columnComments as $columnName => $comment) {
            $columnName = (string) $columnName;
            $definition = $definitions[mb_strtolower($columnName)] ?? null;
            if ($definition === null) {
                $this->logger->warning(sprintf(
                    'Column "%s" was not found in table "%s", its description was not written.',
                    $columnName,
                    $tableName,
                ));
                continue;
            }
            if (preg_match('/\b(VIRTUAL|STORED|PERSISTENT) GENERATED\b/i', (string) $definition['EXTRA'])) {
                $this->logger->warning(sprintf(
                    'Column "%s" in table "%s" is a generated column, its description was not written.',
                    $columnName,
                    $tableName,
                ));
                continue;
            }

            $this->logger->info(sprintf('Setting comment on column "%s" in table "%s"', $columnName, $tableName));
            $columns[$definition['COLUMN_NAME']] = [
                'definition' => $this->queryBuilder->columnDefinition($this->connection, $definition, $isMariaDb),
                'comment' => $this->truncateComment(
                    $comment,
                    self::COLUMN_COMMENT_MAX_LENGTH,
                    sprintf('column "%s" in table "%s"', $columnName, $tableName),
                ),
            ];
        }

        if ($columns === []) {
            return;
        }
        $this->connection->exec(
            $this->queryBuilder->modifyColumnCommentsQueryStatement($this->connection, $tableName, $columns),
        );
    }

    /**
     * @return array{
     *     COLUMN_NAME: string,
     *     COLUMN_TYPE: string,
     *     IS_NULLABLE: string,
     *     COLUMN_DEFAULT: ?string,
     *     EXTRA: ?string,
     *     CHARACTER_SET_NAME: ?string,
     *     COLLATION_NAME: ?string,
     * }[]
     */
    private function fetchColumnDefinitions(string $tableName): array
    {
        /** @var array{
         *     COLUMN_NAME: string,
         *     COLUMN_TYPE: string,
         *     IS_NULLABLE: string,
         *     COLUMN_DEFAULT: ?string,
         *     EXTRA: ?string,
         *     CHARACTER_SET_NAME: ?string,
         *     COLLATION_NAME: ?string,
         * }[] $rows
         */
        $rows = $this->connection->fetchAll(
            $this->queryBuilder->columnDefinitionsQueryStatement($this->connection, $tableName),
        );
        return $rows;
    }

    private function isMariaDb(): bool
    {
        /** @var array{version: string}[] $rows */
        $rows = $this->connection->fetchAll('SELECT VERSION() AS version');
        return str_contains(strtolower($rows[0]['version'] ?? ''), 'mariadb');
    }

    private function truncateComment(string $comment, int $maxLength, string $target): string
    {
        if (mb_strlen($comment) <= $maxLength) {
            return $comment;
        }
        $this->logger->warning(sprintf(
            'Description of %s is longer than %d characters, it was truncated.',
            $target,
            $maxLength,
        ));
        return mb_substr($comment, 0, $maxLength);
    }
}
