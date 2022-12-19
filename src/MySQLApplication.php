<?php

declare(strict_types=1);

namespace Keboola\DbWriter;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Configuration\MySQLActionConfigRowDefinition;
use Keboola\DbWriter\Configuration\MySQLConfigDefinition;
use Keboola\DbWriter\Configuration\MySQLConfigRowDefinition;
use Keboola\DbWriter\Writer\MySQL;

class MySQLApplication extends Application
{

    public function __construct(array $config, Logger $logger)
    {
        $action = $config['action'] ?? 'run';
        if (isset($config['parameters']['tables'])) {
            $configDefinition = new MySQLConfigDefinition();
        } else {
            if ($action === 'run') {
                $configDefinition = new MySQLConfigRowDefinition;
            } else {
                $configDefinition = new MySQLActionConfigRowDefinition();
            }
        }
        parent::__construct($config, $logger, $configDefinition);
    }

    public function writeFull(CsvFile $csv, array $tableConfig): void
    {
        /** @var MySQL $writer */
        $writer = $this['writer'];

        $writer->truncate($tableConfig['dbName']);
        $writer->write($csv, $tableConfig);
    }

    public function writeIncremental(CsvFile $csv, array $tableConfig): void
    {
        /** @var WriterInterface $writer */
        $writer = $this['writer'];
$this['logger']->info('A');
        // write to staging table
        $stageTable = $tableConfig;
        $stageTable['dbName'] = $writer->generateTmpName($tableConfig['dbName']);
        $stageTable['temporary'] = true;
$this['logger']->info('B');
        $writer->create($stageTable);
$this['logger']->info('C');
        $writer->write($csv, $stageTable);
$this['logger']->info('D');
        // create destination table if not exists
        $dstTableExists = $writer->tableExists($tableConfig['dbName']);
$this['logger']->info('E');
        if (!$dstTableExists) {
            $this['logger']->info('F');
            $writer->create($tableConfig);
        }
        $this['logger']->info('G');
        $writer->validateTable($tableConfig);
        $this['logger']->info('H');
        // upsert from staging to destination table
        $writer->upsert($stageTable, $tableConfig['dbName']);
    }
}
