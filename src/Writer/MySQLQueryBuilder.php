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
