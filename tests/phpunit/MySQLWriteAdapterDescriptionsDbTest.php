<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Writer\MySQLConnection;
use Keboola\DbWriter\Writer\MySQLConnectionFactory;
use Keboola\DbWriter\Writer\MySQLQueryBuilder;
use Keboola\DbWriter\Writer\MySQLWriteAdapter;
use Keboola\DbWriterAdapter\ValueObject\TableDescriptions;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class MySQLWriteAdapterDescriptionsDbTest extends TestCase
{
    private const TABLE_NAME = 'descriptions `test`';

    private MySQLConnection $connection;

    protected function setUp(): void
    {
        $this->connection = MySQLConnectionFactory::create(DatabaseConfig::fromArray([
            'host' => (string) getenv('DB_HOST'),
            'port' => (string) getenv('DB_PORT'),
            'database' => (string) getenv('DB_DATABASE'),
            'user' => (string) getenv('DB_USER'),
            '#password' => (string) getenv('DB_PASSWORD'),
        ]), new TestLogger());
        $this->dropTable();
    }

    protected function tearDown(): void
    {
        $this->dropTable();
    }

    public function testColumnDefinitionsArePreserved(): void
    {
        $table = $this->connection->quoteIdentifier(self::TABLE_NAME);
        $this->connection->exec(sprintf(
            'CREATE TABLE %s (
                `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `code` VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT \'it\'\'s "x" \\\\\',
                `note` VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL,
                `amount` DECIMAL(10,2) NOT NULL DEFAULT \'0.00\',
                `flag` BIT(1) NOT NULL DEFAULT b\'1\',
                `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
                `status` ENUM(\'a\', \'b\'\'c\') NOT NULL DEFAULT \'b\'\'c\',
                `untouched` INT COMMENT \'Keep me\',
                KEY `code_idx` (`code`)
            ) COMMENT = \'Old table comment\'',
            $table,
        ));
        if ($this->isMySql8()) {
            $this->connection->exec(sprintf(
                'ALTER TABLE %s'
                . ' ADD COLUMN `expr` VARCHAR(50) NOT NULL DEFAULT (CONCAT(\'x\\\\y\', \'it\'\'s\', \'q"\')),'
                . ' ADD COLUMN `sum` INT DEFAULT (1 + 1)',
                $table,
            ));
        }
        $columnsBefore = $this->columnsWithoutComments();
        $indexesBefore = $this->connection->fetchAll(sprintf('SHOW INDEX FROM %s', $table));

        $comments = [];
        foreach (array_keys($columnsBefore) as $column) {
            if ($column !== 'untouched') {
                $comments[$column] = sprintf('Column %s: it\'s "quoted" \\', $column);
            }
        }
        $logger = new TestLogger();
        $adapter = new MySQLWriteAdapter($this->connection, new MySQLQueryBuilder('utf8mb4'), $logger);
        $adapter->applyDescriptions(self::TABLE_NAME, new TableDescriptions('Table: it\'s "new"', $comments));

        self::assertSame($columnsBefore, $this->columnsWithoutComments());
        self::assertEquals($indexesBefore, $this->connection->fetchAll(sprintf('SHOW INDEX FROM %s', $table)));
        $expectedComments = $comments + ['untouched' => 'Keep me'];
        $actualComments = array_column($this->columns(), 'COLUMN_COMMENT', 'COLUMN_NAME');
        ksort($expectedComments);
        ksort($actualComments);
        self::assertSame($expectedComments, $actualComments);
        /** @var array{TABLE_COMMENT: string}[] $tableInfo */
        $tableInfo = $this->connection->fetchAll(sprintf(
            'SELECT TABLE_COMMENT AS TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES '
            . 'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s',
            $this->connection->quote(self::TABLE_NAME),
        ));
        self::assertSame('Table: it\'s "new"', $tableInfo[0]['TABLE_COMMENT']);
        self::assertFalse($logger->hasWarningRecords());
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function columnsWithoutComments(): array
    {
        $columns = [];
        foreach ($this->columns() as $column) {
            unset($column['COLUMN_COMMENT']);
            $columns[$column['COLUMN_NAME']] = $column;
        }
        return $columns;
    }

    /**
     * @return array<int, array{COLUMN_NAME: string, COLUMN_COMMENT: string}&array<string, mixed>>
     */
    private function columns(): array
    {
        /** @var array<int, array{COLUMN_NAME: string, COLUMN_COMMENT: string}&array<string, mixed>> $columns */
        $columns = $this->connection->fetchAll(sprintf(
            'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s '
            . 'ORDER BY ORDINAL_POSITION',
            $this->connection->quote(self::TABLE_NAME),
        ));
        return $columns;
    }

    private function isMySql8(): bool
    {
        /** @var array{version: string}[] $version */
        $version = $this->connection->fetchAll('SELECT VERSION() AS version');
        return (int) $version[0]['version'] >= 8;
    }

    private function dropTable(): void
    {
        $this->connection->exec(sprintf(
            'DROP TABLE IF EXISTS %s',
            $this->connection->quoteIdentifier(self::TABLE_NAME),
        ));
    }
}
