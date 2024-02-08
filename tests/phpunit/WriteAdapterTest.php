<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\Csv\CsvWriter;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer\MySQLConnection;
use Keboola\DbWriter\Writer\MySQLConnectionFactory;
use Keboola\DbWriter\Writer\MySQLQueryBuilder;
use Keboola\DbWriter\Writer\MySQLWriteAdapter;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Temp\Temp;
use PDO;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem;

class WriteAdapterTest extends TestCase
{
    public function testUpsert(): void
    {
        $logger = new TestLogger();
        $exportConfig = $this->buildExportConfig();
        $connection = MySQLConnectionFactory::create(
            $exportConfig->getDatabaseConfig(),
            $logger,
        );

        $adapter = new MySQLWriteAdapter($connection, new MySQLQueryBuilder('utf8mb4'), $logger);

        $adapter->create(
            $exportConfig->getDbName() . '_tmp',
            true,
            $exportConfig->getItems(),
            $exportConfig->getPrimaryKey(),
        );

        if ($adapter->tableExists($exportConfig->getDbName())) {
            $adapter->drop($exportConfig->getDbName());
        }

        $adapter->create(
            $exportConfig->getDbName(),
            false,
            $exportConfig->getItems(),
            $exportConfig->getPrimaryKey(),
        );

        $adapter->upsert($exportConfig, $exportConfig->getDbName() . '_tmp');

        self::assertTrue($logger->hasInfo('Upserting data into table "test"'));
        self::assertTrue($logger->hasDebug('Running query "SHOW KEYS FROM `test` WHERE Key_name = "PRIMARY"".'));
        self::assertTrue($logger->hasInfo('Upserted data into table "test"'));
    }

    public function testThrowPrimaryKeysException(): void
    {
        $logger = new TestLogger();
        $exportConfig = $this->buildExportConfig();
        $connection = MySQLConnectionFactory::create(
            $exportConfig->getDatabaseConfig(),
            $logger,
        );

        $adapter = new MySQLWriteAdapter($connection, new MySQLQueryBuilder('utf8mb4'), $logger);

        $adapter->create(
            $exportConfig->getDbName() . '_tmp',
            true,
            $exportConfig->getItems(),
            $exportConfig->getPrimaryKey(),
        );

        if ($adapter->tableExists($exportConfig->getDbName())) {
            $adapter->drop($exportConfig->getDbName());
        }

        $adapter->create(
            $exportConfig->getDbName(),
            false,
            $exportConfig->getItems(),
            ['name'],
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Primary key(s) in configuration does NOT match with keys in DB table.' . PHP_EOL
            . 'Keys in configuration: id' . PHP_EOL
            . 'Keys in DB table: name',
        );
        $adapter->upsert($exportConfig, $exportConfig->getDbName() . '_tmp');
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
                'primaryKey' => ['id'],
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
