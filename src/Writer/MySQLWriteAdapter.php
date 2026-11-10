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

    private const SPATIAL_TYPE_PATTERN = '/^(geometry|point|linestring|polygon|multipoint|multilinestring'
        . '|multipolygon|geometrycollection)$/i';

    private const LOCK_WAIT_TIMEOUT_SECONDS = 10;

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
            $tableComment = $this->truncateComment(
                $tableComment,
                self::TABLE_COMMENT_MAX_LENGTH,
                sprintf('table "%s"', $tableName),
            );
            // Skip the DDL when the stored comment already matches - ALTER TABLE needs an exclusive
            // metadata lock, so an incremental load must not issue it on every run.
            if ($this->fetchTableComment($tableName) !== $tableComment) {
                $this->logger->info(sprintf('Setting comment on table "%s"', $tableName));
                $this->execDdl($this->queryBuilder->tableCommentQueryStatement(
                    $this->connection,
                    $tableName,
                    $tableComment,
                ));
            }
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
            // INFORMATION_SCHEMA.COLUMNS.COLUMN_TYPE does not carry the SRID of a spatial column,
            // so restating the definition would silently drop it.
            if (preg_match(self::SPATIAL_TYPE_PATTERN, $definition['COLUMN_TYPE'])) {
                $this->logger->warning(sprintf(
                    'Column "%s" in table "%s" is a spatial column, its description was not written.',
                    $columnName,
                    $tableName,
                ));
                continue;
            }

            $comment = $this->truncateComment(
                $comment,
                self::COLUMN_COMMENT_MAX_LENGTH,
                sprintf('column "%s" in table "%s"', $columnName, $tableName),
            );
            if ($definition['COLUMN_COMMENT'] === $comment) {
                continue;
            }

            $this->logger->info(sprintf('Setting comment on column "%s" in table "%s"', $columnName, $tableName));
            $columns[$definition['COLUMN_NAME']] = [
                'definition' => $this->queryBuilder->columnDefinition($this->connection, $definition, $isMariaDb),
                'comment' => $comment,
            ];
        }

        if ($columns === []) {
            return;
        }
        $this->execDdl(
            $this->queryBuilder->modifyColumnCommentsQueryStatement($this->connection, $tableName, $columns),
        );
    }

    /**
     * Runs a comment DDL with a short metadata-lock wait, so that a table busy with a long
     * running query makes the comment fail fast instead of blocking every query behind it.
     */
    private function execDdl(string $sql): void
    {
        /** @var array{value: ?string}[] $rows */
        $rows = $this->connection->fetchAll('SELECT @@SESSION.lock_wait_timeout AS value');
        $previous = $rows[0]['value'] ?? null;
        $this->connection->exec(sprintf('SET SESSION lock_wait_timeout = %d', self::LOCK_WAIT_TIMEOUT_SECONDS));
        try {
            $this->connection->exec($sql);
        } finally {
            if ($previous !== null) {
                $this->connection->exec(sprintf('SET SESSION lock_wait_timeout = %d', (int) $previous));
            }
        }
    }

    private function fetchTableComment(string $tableName): ?string
    {
        /** @var array{TABLE_COMMENT: ?string}[] $rows */
        $rows = $this->connection->fetchAll(
            $this->queryBuilder->tableCommentSelectQueryStatement($this->connection, $tableName),
        );
        return $rows[0]['TABLE_COMMENT'] ?? null;
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
     *     COLUMN_COMMENT: ?string,
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
         *     COLUMN_COMMENT: ?string,
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
