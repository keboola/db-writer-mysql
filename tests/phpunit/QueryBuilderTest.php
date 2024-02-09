<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\Csv\CsvWriter;
use Keboola\DbWriter\Writer\MySQLConnection;
use Keboola\DbWriter\Writer\MySQLConnectionFactory;
use Keboola\DbWriter\Writer\MySQLQueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem;

class QueryBuilderTest extends TestCase
{
    private MySQLConnection $connection;

    public function __construct(?string $name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        $this->connection = MySQLConnectionFactory::create(
            $this->buildExportConfig()->getDatabaseConfig(),
            new TestLogger(),
        );
    }

    public function testUpsertWithPK(): void
    {
        $queryBuilder = new MySQLQueryBuilder('utf8mb4');
        $query = $queryBuilder->upsertWithPrimaryKeys(
            $this->connection,
            $this->buildExportConfig(),
            'testTableName',
        );

        // disable phpcs because of long string
        /** @phpcs:disable */
        $this->assertEquals(
            'INSERT INTO `test` (`id`, `name`, `age`) SELECT * FROM `testTableName` ON DUPLICATE KEY UPDATE `test`.`id`=`testTableName`.`id`, `test`.`name`=`testTableName`.`name`, `test`.`age`=`testTableName`.`age`',
            $query,
        );
        /** @phpcs:enable */
    }

    public function testUpsertWithoutPK(): void
    {
        $queryBuilder = new MySQLQueryBuilder('utf8mb4');
        $query = $queryBuilder->upsertWithoutPrimaryKeys(
            $this->connection,
            $this->buildExportConfig(),
            'testTableName',
        );

        $this->assertEquals(
            'INSERT INTO `test` (`id`, `name`, `age`) SELECT * FROM `testTableName`',
            $query,
        );
    }

    public function testWriteData(): void
    {
        $queryBuilder = new MySQLQueryBuilder('utf8mb4');
        $exportConfig = $this->buildExportConfig();
        $query = $queryBuilder->writeDataQueryStatement(
            $this->connection,
            'testTableName',
            $exportConfig,
        );

        $expectedQuery = <<<SQL
LOAD DATA LOCAL INFILE '%s/in/tables/test.csv'
INTO TABLE `testTableName`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY ''
IGNORE 1 LINES
(`id`, `name`, `age`)

SQL;

        $this->assertEquals(
            sprintf($expectedQuery, $exportConfig->getDataDir()),
            $query,
        );
    }

    public function testWriteDataWithIgnoreColumn(): void
    {
        $items = [
            [
                'name' => 'id',
                'dbName' => 'id',
                'type' => 'int',
                'size' => null,
                'nullable' => false,
            ],
            [
                'name' => 'name',
                'dbName' => 'name',
                'type' => 'ignore',
                'size' => '255',
                'nullable' => false,
            ],
            [
                'name' => 'age',
                'dbName' => 'age',
                'type' => 'ignore',
                'nullable' => false,
            ],
        ];
        $queryBuilder = new MySQLQueryBuilder('utf8mb4');
        $exportConfig = $this->buildExportConfig($items);
        $query = $queryBuilder->writeDataQueryStatement(
            $this->connection,
            'testTableName',
            $exportConfig,
        );

        $expectedQuery = <<<SQL
LOAD DATA LOCAL INFILE '%s/in/tables/test.csv'
INTO TABLE `testTableName`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY ''
IGNORE 1 LINES
(`id`, @dummy, @dummy)

SQL;

        $this->assertEquals(
            sprintf($expectedQuery, $exportConfig->getDataDir()),
            $query,
        );
    }

    public function testWriteDataWithNullableColumn(): void
    {
        $items = [
            [
                'name' => 'id',
                'dbName' => 'id',
                'type' => 'int',
                'size' => null,
                'nullable' => false,
            ],
            [
                'name' => 'name',
                'dbName' => 'name',
                'type' => 'varchar',
                'size' => '255',
                'nullable' => true,
            ],
            [
                'name' => 'age',
                'dbName' => 'age',
                'type' => 'int',
                'nullable' => true,
            ],
        ];
        $queryBuilder = new MySQLQueryBuilder('utf8mb4');
        $exportConfig = $this->buildExportConfig($items);
        $query = $queryBuilder->writeDataQueryStatement(
            $this->connection,
            'testTableName',
            $exportConfig,
        );

        $expectedQuery = <<<SQL
LOAD DATA LOCAL INFILE '%s/in/tables/test.csv'
INTO TABLE `testTableName`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY ''
IGNORE 1 LINES
(`id`, @columnVar_1, @columnVar_2)
SET `name` = IF(@columnVar_1 = '', NULL, @columnVar_1),`age` = IF(@columnVar_2 = '', NULL, @columnVar_2)
SQL;

        $this->assertEquals(
            sprintf($expectedQuery, $exportConfig->getDataDir()),
            $query,
        );
    }

    public function testWriteDataWithDefaultColumnValue(): void
    {
        $items = [
            [
                'name' => 'id',
                'dbName' => 'id',
                'type' => 'int',
                'size' => null,
                'nullable' => false,
            ],
            [
                'name' => 'name',
                'dbName' => 'name',
                'type' => 'varchar',
                'size' => '255',
                'nullable' => false,
                'default' => 'test',
            ],
            [
                'name' => 'age',
                'dbName' => 'age',
                'type' => 'int',
                'nullable' => true,
                'default' => '1',
            ],
        ];
        $queryBuilder = new MySQLQueryBuilder('utf8mb4');
        $exportConfig = $this->buildExportConfig($items);
        $query = $queryBuilder->writeDataQueryStatement(
            $this->connection,
            'testTableName',
            $exportConfig,
        );

        $expectedQuery = <<<SQL
LOAD DATA LOCAL INFILE '%s/in/tables/test.csv'
INTO TABLE `testTableName`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY ''
IGNORE 1 LINES
(`id`, @columnVar_1, @columnVar_2)
SET `name` = IF(@columnVar_1 = '', 'test', @columnVar_1),`age` = IF(@columnVar_2 = '', '1', @columnVar_2)
SQL;

        $this->assertEquals(
            sprintf($expectedQuery, $exportConfig->getDataDir()),
            $query,
        );
    }



    public function testListPrimaryKeys(): void
    {
        $queryBuilder = new MySQLQueryBuilder('utf8mb4');
        $query = $queryBuilder->getPrimaryKeysQuery(
            $this->connection,
            'testTableName',
        );

        $this->assertEquals(
            'SHOW KEYS FROM `testTableName` WHERE Key_name = "PRIMARY"',
            $query,
        );
    }

    private function buildExportConfig(?array $items = null): ExportConfig
    {
        $tmp = new Temp();
        $fs = new Filesystem();
        $dataDir = $tmp->getTmpFolder();
        if (!$fs->exists($dataDir . '/in/tables/')) {
            $fs->mkdir($dataDir . '/in/tables/');
        }
        $csv = new CsvWriter($dataDir . '/in/tables/test.csv');
        $csv->writeRow(['id', 'name', 'age']);

        return ExportConfig::fromArray(
            [
                'data_dir' => $dataDir,
                'writer_class' => 'MySQL',
                'dbName' => 'test',
                'tableId' => 'test',
                'db' => [
                    'host' => (string) getenv('DB_HOST'),
                    'port' => (string) getenv('DB_PORT'),
                    'database' => (string) getenv('DB_DATABASE'),
                    'user' => (string) getenv('DB_USER'),
                    '#password' => (string) getenv('DB_PASSWORD'),
                ],
                'items' => $items ?? [
                    [
                        'name' => 'id',
                        'dbName' => 'id',
                        'type' => 'int',
                        'size' => null,
                        'nullable' => false,
                    ],
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'varchar',
                        'size' => '255',
                        'nullable' => false,
                    ],
                    [
                        'name' => 'age',
                        'dbName' => 'age',
                        'type' => 'int',
                        'nullable' => false,
                    ],
                ],
            ],
            [
                [
                    'source' => 'test',
                    'destination' => 'test.csv',
                    'columns' => ['id', 'name', 'age'],
                ],
            ],
        );
    }
}
