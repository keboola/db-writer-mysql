<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvReader;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterAdapter\Query\DefaultQueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use Keboola\DbWriterConfig\Exception\PropertyNotSetException;

class MySQLQueryBuilder extends DefaultQueryBuilder
{
    private array $variableColumns = [];

    public function __construct(protected string $charset)
    {
    }

    /**
     * @param ItemConfig[] $items
     * @throws PropertyNotSetException
     */
    public function createQueryStatement(
        Connection $connection,
        string $tableName,
        bool $isTempTable,
        array $items,
        ?array $primaryKeys = null,
    ): string {
        $createTable = sprintf(
            'CREATE %s TABLE `%s`',
            $isTempTable ? 'TEMPORARY' : '',
            $tableName,
        );

        $filteredItems = array_filter(
            $items,
            fn(ItemConfig $itemConfig) => strtolower($itemConfig->getType()) !== 'ignore',
        );

        $columnsDefinition = array_map(
            fn(ItemConfig $itemConfig) => sprintf(
                '%s %s%s %s %s',
                $connection->quoteIdentifier($itemConfig->getDbName()),
                $itemConfig->getType(),
                $itemConfig->hasSize() ? sprintf('(%s)', $itemConfig->getSize()) : '',
                $itemConfig->getNullable() ? 'NULL' : 'NOT NULL',
                $itemConfig->hasDefault() && $itemConfig->getType() !== 'TEXT' ?
                    'DEFAULT ' . $this->createColumnDefault($connection, $itemConfig) :
                    '',
            ),
            $filteredItems,
        );

        if ($primaryKeys) {
            $columnsDefinition[] = sprintf(
                'PRIMARY KEY (%s)',
                implode(',', array_map(fn($item) => $connection->quoteIdentifier($item), $primaryKeys)),
            );
        }

        return sprintf(
            '%s (%s) DEFAULT CHARSET=%s COLLATE=%s_unicode_ci;',
            $createTable,
            implode(',', $columnsDefinition),
            $this->charset,
            $this->charset,
        );
    }

    public function writeDataQueryStatement(
        Connection $connection,
        string $tableName,
        ExportConfig $exportConfig,
    ): string {
        $csvFile = new CsvReader($exportConfig->getTableFilePath());
        $header = $csvFile->getHeader();
        $columnNames = $this->columnNamesForLoad($connection, $exportConfig->getItems(), $header);

        $query = <<<SQL
LOAD DATA LOCAL INFILE '%s'
INTO TABLE %s
CHARACTER SET %s
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY ''
IGNORE 1 LINES
(%s)
%s
SQL;
        return sprintf(
            $query,
            $exportConfig->getTableFilePath(),
            $connection->quoteIdentifier($tableName),
            $this->charset,
            implode(', ', $columnNames),
            $this->getExpressionReplace($connection, $exportConfig->getItems()),
        );
    }

    /**
     * @param ItemConfig[] $items
     */
    private function getExpressionReplace(Connection $connection, array $items): string
    {
        $expressions = array_merge(
            $this->emptyToNullOrDefault($connection, $items),
            $this->replaceColumnsValues($connection, $items),
        );

        if (!$expressions) {
            return '';
        }

        return 'SET ' . implode(',', ($expressions));
    }

    public function upsertWithPrimaryKeys(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        $valuesClauseArr = [];
        foreach ($exportConfig->getItems() as $item) {
            $valuesClauseArr[] = sprintf(
                '%s.%s=%s.%s',
                $connection->quoteIdentifier($exportConfig->getDbName()),
                $connection->quoteIdentifier($item->getDbName()),
                $connection->quoteIdentifier($stageTableName),
                $connection->quoteIdentifier($item->getDbName()),
            );
        }

        return sprintf(
            'INSERT INTO %s (%s) SELECT * FROM %s ON DUPLICATE KEY UPDATE %s',
            $connection->quoteIdentifier($exportConfig->getDbName()),
            implode(
                ', ',
                array_map(
                    fn (ItemConfig $column) => $connection->quoteIdentifier($column->getDbName()),
                    $exportConfig->getItems(),
                ),
            ),
            $connection->quoteIdentifier($stageTableName),
            implode(', ', $valuesClauseArr),
        );
    }

    public function upsertWithoutPrimaryKeys(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        $columns = implode(
            ', ',
            array_map(
                fn (ItemConfig $column) => $connection->quoteIdentifier($column->getDbName()),
                $exportConfig->getItems(),
            ),
        );

        return sprintf(
            'INSERT INTO %s (%s) SELECT * FROM %s',
            $connection->quoteIdentifier($exportConfig->getDbName()),
            $columns,
            $connection->quoteIdentifier($stageTableName),
        );
    }

    public function tableCommentQueryStatement(Connection $connection, string $tableName, string $comment): string
    {
        return sprintf(
            'ALTER TABLE %s COMMENT = %s',
            $connection->quoteIdentifier($tableName),
            $connection->quote($comment),
        );
    }

    public function tableCommentSelectQueryStatement(Connection $connection, string $tableName): string
    {
        return sprintf(
            'SELECT TABLE_COMMENT AS TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES '
            . 'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s',
            $connection->quote($tableName),
        );
    }

    public function columnDefinitionsQueryStatement(Connection $connection, string $tableName): string
    {
        return sprintf(
            'SELECT COLUMN_NAME AS COLUMN_NAME, COLUMN_TYPE AS COLUMN_TYPE, IS_NULLABLE AS IS_NULLABLE, '
            . 'COLUMN_DEFAULT AS COLUMN_DEFAULT, EXTRA AS EXTRA, CHARACTER_SET_NAME AS CHARACTER_SET_NAME, '
            . 'COLLATION_NAME AS COLLATION_NAME, COLUMN_COMMENT AS COLUMN_COMMENT '
            . 'FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s '
            . 'ORDER BY ORDINAL_POSITION',
            $connection->quote($tableName),
        );
    }

    /**
     * Rebuilds a column definition (without name and comment) from an INFORMATION_SCHEMA.COLUMNS row.
     *
     * @param array{
     *     COLUMN_TYPE: string,
     *     IS_NULLABLE: string,
     *     COLUMN_DEFAULT: ?string,
     *     EXTRA: ?string,
     *     CHARACTER_SET_NAME: ?string,
     *     COLLATION_NAME: ?string,
     * } $column
     */
    public function columnDefinition(Connection $connection, array $column, bool $isMariaDb): string
    {
        $definition = $column['COLUMN_TYPE'];
        if ($column['CHARACTER_SET_NAME'] !== null && $column['COLLATION_NAME'] !== null) {
            $definition .= sprintf(
                ' CHARACTER SET %s COLLATE %s',
                $column['CHARACTER_SET_NAME'],
                $column['COLLATION_NAME'],
            );
        }
        $definition .= $column['IS_NULLABLE'] === 'YES' ? ' NULL' : ' NOT NULL';

        $default = $this->columnDefaultExpression($connection, $column, $isMariaDb);
        if ($default !== null) {
            $definition .= ' DEFAULT ' . $default;
        }

        // DEFAULT_GENERATED is an INFORMATION_SCHEMA marker, not valid column syntax.
        $extra = trim((string) preg_replace('/\bDEFAULT_GENERATED\b/i', '', (string) $column['EXTRA']));
        if ($extra !== '') {
            $definition .= ' ' . $extra;
        }

        return $definition;
    }

    /**
     * @param array<string, array{definition: string, comment: string}> $columns keyed by column name
     */
    public function modifyColumnCommentsQueryStatement(
        Connection $connection,
        string $tableName,
        array $columns,
    ): string {
        $modifications = [];
        foreach ($columns as $columnName => $column) {
            $modifications[] = sprintf(
                'MODIFY COLUMN %s %s COMMENT %s',
                $connection->quoteIdentifier((string) $columnName),
                $column['definition'],
                $connection->quote($column['comment']),
            );
        }

        return sprintf(
            'ALTER TABLE %s %s',
            $connection->quoteIdentifier($tableName),
            implode(', ', $modifications),
        );
    }

    public function getPrimaryKeysQuery(Connection $connection, string $tableName): string
    {
        return sprintf(
            'SHOW KEYS FROM %s WHERE Key_name = "PRIMARY"',
            $connection->quoteIdentifier($tableName),
        );
    }

    /**
     * @param ItemConfig[] $items
     */
    private function columnNamesForLoad(Connection $connection, array $items, array $header): array
    {
        return array_map(function ($column) use ($items, $connection) {
            // skip ignored
            foreach ($items as $item) {
                if ($item->getName() === $column && strtolower($item->getType()) === 'ignore') {
                    return '@dummy';
                }
            }

            // name by mapping
            foreach ($items as $key => $item) {
                if ($item->getName() === $column) {
                    if ($this->hasColumnNullOrDefault($item) ||
                        $this->hasColumnForReplaceValues($item)
                    ) {
                        return $this->generateColumnVariable($item->getDbName(), $key);
                    }
                    return $connection->quoteIdentifier($item->getDbName());
                }
            }

            // origin sapi name
            return $connection->quoteIdentifier($column);
        }, $header);
    }

    private function hasColumnNullOrDefault(ItemConfig $item): bool
    {
        try {
            $default = $item->getDefault();
        } catch (PropertyNotSetException $e) {
            $default = false;
        }

        $isIgnored = strtolower($item->getType()) === 'ignore';
        return !$isIgnored && ($default !== false || $item->getNullable());
    }

    private function hasColumnForReplaceValues(ItemConfig $item): bool
    {
        return strtolower($item->getType()) === 'bit';
    }

    private function generateColumnVariable(string $columnName, int|string $key): string
    {
        $variable = '@columnVar_' . $key;
        $this->variableColumns[$columnName] = $variable;
        return $variable;
    }

    /**
     * @param ItemConfig[] $items
     * @throws ApplicationException|PropertyNotSetException
     */
    private function emptyToNullOrDefault(Connection $connection, array $items): array
    {
        $columnsWithNullOrDefault = array_filter($items, function (ItemConfig $column) {
            return $this->hasColumnNullOrDefault($column);
        });

        $result = [];
        foreach ($columnsWithNullOrDefault as $column) {
            switch (strtolower($column->getType())) {
                case 'date':
                case 'datetime':
                    $defaultValue = $column->hasDefault() ? $connection->quote($column->getDefault()) : 'NULL';
                    break;
                default:
                    try {
                        $default = $column->getDefault();
                    } catch (PropertyNotSetException) {
                        $default = '';
                    }
                    if ($default !== '') {
                        $defaultValue = $connection->quote($default);
                    } elseif ($column->getNullable()) {
                        $defaultValue = 'NULL';
                    } else {
                        $defaultValue = $connection->quote('');
                    }
            }

            $result[] = sprintf(
                "%s = IF(%s = '', %s, %s)",
                $connection->quoteIdentifier($column->getDbName()),
                $this->getVariableColumn($column->getDbName()),
                $defaultValue,
                $this->getVariableColumn($column->getDbName()),
            );
        }

        return $result;
    }

    /**
     * @param ItemConfig[] $items
     * @throws ApplicationException
     */
    private function replaceColumnsValues(Connection $connection, array $items): array
    {
        $result = [];
        foreach ($items as $column) {
            switch (strtolower($column->getType())) {
                case 'bit':
                    if ($column->getNullable()) {
                        $result[] = sprintf(
                            "%s = IF(%s = '', NULL, cast(%s as signed))",
                            $connection->quoteIdentifier($column->getDbName()),
                            $this->getVariableColumn($column->getDbName()),
                            $this->getVariableColumn($column->getDbName()),
                        );
                    } else {
                        $result[] = sprintf(
                            '%s = cast(%s as signed)',
                            $connection->quoteIdentifier($column->getDbName()),
                            $this->getVariableColumn($column->getDbName()),
                        );
                    }
            }
        }

        return $result;
    }

    /**
     * @throws PropertyNotSetException
     */
    private function createColumnDefault(Connection $connection, ItemConfig $item): string
    {
        $default = $item->getDefault();
        // MySQL rejects a quoted string as a BIT default (error 1067).
        if (strtolower($item->getType()) === 'bit' && preg_match('/^[01]+$/', $default)) {
            return sprintf("b'%s'", $default);
        }

        return $connection->quote($default);
    }

    /**
     * @param array{COLUMN_TYPE: string, COLUMN_DEFAULT: ?string, EXTRA: ?string} $column
     */
    private function columnDefaultExpression(Connection $connection, array $column, bool $isMariaDb): ?string
    {
        $default = $column['COLUMN_DEFAULT'];
        if ($default === null) {
            return null;
        }

        // MariaDB returns defaults as SQL expressions: literals quoted, NULL as the word NULL.
        if ($isMariaDb) {
            return strtoupper($default) === 'NULL' ? null : $default;
        }

        if (preg_match('/^(CURRENT_TIMESTAMP|NOW|LOCALTIME|LOCALTIMESTAMP)(\(\d*\))?$/i', $default)) {
            return $default;
        }
        if (preg_match('/\bDEFAULT_GENERATED\b/i', (string) $column['EXTRA'])) {
            // INFORMATION_SCHEMA returns expression defaults with quotes and backslashes escaped once more.
            return sprintf('(%s)', preg_replace('/\\\\([\\\\\'])/', '$1', $default));
        }
        $type = strtolower($column['COLUMN_TYPE']);
        if (str_starts_with($type, 'bit') && preg_match("/^b'[01]*'$/", $default)) {
            return $default;
        }
        $isBinaryType = (bool) preg_match('/^((var)?binary|(tiny|medium|long)?blob)\b/', $type);
        if ($isBinaryType && preg_match('/^0x[0-9a-f]*$/i', $default)) {
            return $default;
        }

        return $connection->quote($default);
    }

    /**
     * @throws ApplicationException
     */
    private function getVariableColumn(string $columnName): string
    {
        if (!isset($this->variableColumns[$columnName])) {
            throw new ApplicationException(sprintf('Variable for column "%s" does not exists.', $columnName));
        }
        return $this->variableColumns[$columnName];
    }
}
