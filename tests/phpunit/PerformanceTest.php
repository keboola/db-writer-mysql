<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\Csv\CsvWriter;
use Keboola\DbWriter\Writer\MySQLConnectionFactory;
use Keboola\DbWriter\Writer\MySQLQueryBuilder;
use Keboola\DbWriter\Writer\MySQLWriteAdapter;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem;

class PerformanceTest extends TestCase
{

    public function testWriteMilionRows(): void
    {
        $temp = new Temp();

        $config = <<<JSON
{
    "parameters": {
        "data_dir": "%s",
        "writer_class": "mysql",
        "db": {
            "host": "%s",
            "port": "%s",
            "database": "%s",
            "user": "%s",
            "#password": "%s"
        },
        "export": true,
        "tableId": "performance",
        "dbName": "performance",
        "items": [
            {
                "name": "col-1",
                "dbName": "col-1",
                "type": "int",
                "size": null,
                "nullable": true,
                "default": null
            },
            {
                "name": "col-2",
                "dbName": "col-2",
                "type": "varchar",
                "size": "255",
                "nullable": true,
                "default": ""
            },
            {
                "name": "col-3",
                "dbName": "col-3",
                "type": "varchar",
                "size": "255",
                "nullable": true,
                "default": ""
            },
            {
                "name": "col-4",
                "dbName": "col-4",
                "type": "varchar",
                "size": "255",
                "nullable": true,
                "default": "default"
            },
            {
                "name": "col-5",
                "dbName": "col-5",
                "type": "varchar",
                "size": "255",
                "nullable": false,
                "default": ""
            },
            {
                "name": "col-6",
                "dbName": "col-6",
                "type": "varchar",
                "size": "255",
                "nullable": false,
                "default": "default"
            },
            {
                "name": "col-7",
                "dbName": "col-7",
                "type": "int",
                "size": null,
                "nullable": true,
                "default": "123"
            },
            {
                "name": "col-8",
                "dbName": "col-8",
                "type": "date",
                "size": null,
                "nullable": true,
                "default": ""
            },
            {
                "name": "col-9",
                "dbName": "col-9",
                "type": "datetime",
                "size": null,
                "nullable": true,
                "default": ""
            },
            {
                "name": "col-10",
                "dbName": "col-10",
                "type": "int",
                "size": "255",
                "nullable": false,
                "default": ""
            }
        ]
    },
    "storage": {
        "input": {
            "tables": [
                {
                    "source": "performance",
                    "destination": "performance.csv"
                }
            ]
        }
    }
}
JSON;

        /** @var array{
         *   "parameters": array,
         *   "storage": array
         * } $config
         */
        $config = (array) json_decode(
            sprintf(
                (string) $config,
                $temp->getTmpFolder(),
                getenv('DB_HOST'),
                getenv('DB_PORT'),
                getenv('DB_DATABASE'),
                getenv('DB_USER'),
                getenv('DB_PASSWORD'),
            ),
            true,
        );

        $fs = new Filesystem();
        $fs->mkdir($temp->getTmpFolder() . '/in/tables');

        $csv = new CsvWriter($temp->getTmpFolder() . '/in/tables/performance.csv');
        $csv->writeRow([
            'col-1',
            'col-2',
            'col-3',
            'col-4',
            'col-5',
            'col-6',
            'col-7',
            'col-8',
            'col-9',
            'col-10',
        ]);
        for ($i = 0; $i < 1000000; $i++) {
            $csv->writeRow([
                'col-1' => '',
                'col-2' => 'col2',
                'col-3' => 'col3',
                'col-4' => 'col4',
                'col-5' => 'col5',
                'col-6' => 'col6',
                'col-7' => '123',
                'col-8' => '2020-06-04',
                'col-9' => '2020-06-04 16:43:12',
                'col-10' => '12',
            ]);
            $csv->writeRow([
                'col-1' => '',
                'col-2' => '',
                'col-3' => '',
                'col-4' => '',
                'col-5' => '',
                'col-6' => '',
                'col-7' => '',
                'col-8' => '',
                'col-9' => '',
                'col-10' => '',
            ]);
        }

        $exportConfig = ExportConfig::fromArray(
            $config['parameters'],
            $config['storage']['input']['tables'],
        );

        $testLogger = new TestLogger();
        $adapter = new MySQLWriteAdapter(
            MySQLConnectionFactory::create(
                $exportConfig->getDatabaseConfig(),
                $testLogger,
            ),
            new MySQLQueryBuilder('utf8mb4'),
            $testLogger,
        );
        if ($adapter->tableExists($exportConfig->getDbName())) {
            $adapter->drop($exportConfig->getDbName());
        }
        $adapter->create($exportConfig->getDbName(), false, $exportConfig->getItems());

        $startTime = microtime(true);
        $adapter->writeData($exportConfig->getDbName(), $exportConfig);
        $stopTime = microtime(true);
        self::assertLessThan(50, round($stopTime-$startTime));
    }
}
