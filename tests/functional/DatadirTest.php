<?php

declare(strict_types=1);

namespace Keboola\DbWriter\FunctionalTests;

use Keboola\Csv\CsvFile;
use Keboola\DatadirTests\AbstractDatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use Keboola\DatadirTests\DatadirTestsProviderInterface;
use \PDO;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class DatadirTest extends AbstractDatadirTestCase
{

    /** @var PDO $connection */
    private $connection;

    /** @var array $config */
    private $config;

    /** @var string $dataDir */
    private $dataDir;

    public function setUp(): void
    {
        parent::setUp();
        $this->config = $this->getDatabaseConfig();
        $this->connection = $this->createConnection($this->config);
        $this->dropTables();
    }

    /**
     * @dataProvider provideDatadirSpecifications
     */
    public function testDatadir(DatadirTestSpecificationInterface $specification): void
    {
        $tempDatadir = $this->getTempDatadir($specification);
        $this->dataDir = $tempDatadir->getTmpFolder();

        $this->dropTables();

        $process = $this->runScript($tempDatadir->getTmpFolder());

        $this->dumpTables($tempDatadir->getTmpFolder());

        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
    }

    /**
     * @return DatadirTestsProviderInterface[]
     */
    protected function getDataProviders(): array
    {
        return [
            new DatadirTestProvider($this->getTestFileDir()),
        ];
    }

    protected function getScript(): string
    {
        return sprintf(
            '%s/../../run.php --data=%s/',
            $this->getTestFileDir(),
            $this->dataDir
        );
    }

    protected function runScript(string $datadirPath): Process
    {
        $script = $this->getScript();

        $runCommand = 'php ' . $script;
        $runProcess = Process::fromShellCommandline($runCommand);
        $runProcess->setTimeout(0);
        $runProcess->run();
        return $runProcess;
    }

    private function dropTables(): void
    {
        foreach ($this->getTableNames() as $tableName) {
            $this->connection->query(sprintf('DROP TABLE IF EXISTS `%s`', $tableName))->execute();
        }
    }

    private function dumpTables(string $tmpFolder): void
    {
        $dumpDir = $tmpFolder . '/out/db-dump';
        $fs = new Filesystem();
        $fs->mkdir($dumpDir, 0777);

        foreach ($this->getTableNames() as $tableName) {
            $this->dumpTableData($tableName, $dumpDir);
        }
    }

    private function getTableNames(): array
    {
        $tables = $this->connection->query(
            sprintf(
                'SELECT table_name FROM information_schema.tables WHERE table_schema = \'%s\';',
                $this->config['database']
            )
        )->fetchAll(PDO::FETCH_ASSOC);

        return array_map(function ($item) {
            return $item['table_name'];
        }, $tables);
    }

    private function dumpTableData(string $tableName, string $tmpFolder): void
    {
        $csvDumpFile = new CsvFile(sprintf('%s/%s.csv', $tmpFolder, $tableName));

        $rows = $this->connection->query(sprintf('SELECT * FROM `%s`', $tableName))->fetchAll(PDO::FETCH_ASSOC);
        if ($rows) {
            $csvDumpFile->writeRow(array_keys(current($rows)));
            foreach ($rows as $row) {
                $csvDumpFile->writeRow($row);
            }
        }
    }

    private function createConnection(array $config): PDO
    {
        $dsn = sprintf(
            "mysql:host=%s;port=%s;dbname=%s",
            $config['host'],
            $config['port'],
            $config['database']
        );

        $db = new PDO(
            $dsn,
            $config['user'],
            $config['password']
        );
        $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        return $db;
    }

    private function getDatabaseConfig(): array
    {
        return [
            'host' => getenv('DB_HOST'),
            'port' => getenv('DB_PORT'),
            'database' => getenv('DB_DATABASE'),
            'user' => getenv('DB_USER'),
            'password' => getenv('DB_PASSWORD'),
        ];
    }
}
