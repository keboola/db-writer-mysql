<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Test\MySQLBaseTest;

class PerformanceTest extends MySQLBaseTest
{

    public function testWriteMilionRows(): void
    {
        $config = <<<JSON
{
    "parameters": {
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
                "size": 255,
                "nullable": true,
                "default": ""
            },
            {
                "name": "col-3",
                "dbName": "col-3",
                "type": "varchar",
                "size": 255,
                "nullable": true,
                "default": ""
            },
            {
                "name": "col-4",
                "dbName": "col-4",
                "type": "varchar",
                "size": 255,
                "nullable": true,
                "default": "default"
            },
            {
                "name": "col-5",
                "dbName": "col-5",
                "type": "varchar",
                "size": 255,
                "nullable": false,
                "default": ""
            },
            {
                "name": "col-6",
                "dbName": "col-6",
                "type": "varchar",
                "size": 255,
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
                "size": 255,
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
        $config = sprintf(
            $config,
            getenv('DB_HOST'),
            getenv('DB_PORT'),
            getenv('DB_DATABASE'),
            getenv('DB_USER'),
            getenv('DB_PASSWORD')
        );
        $this->initFixtures(json_decode($config, true), $this->dataDir . '/performance');
        $csv = new CsvFile($this->tmpDataDir . '/in/tables/performance.csv');
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

        $startTime = microtime(true);
        $process = $this->runProcess();
        $stopTime = microtime(true);
        self::assertLessThan(40, round($stopTime-$startTime));
        self::assertEquals(0, $process->getExitCode(), $process->getOutput() . $process->getErrorOutput());
    }
}
