<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Test;

use Keboola\DbWriter\Writer\MySQL;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class MySQLBaseTest extends BaseTest
{
    /** @var string */
    private $rootDir = __DIR__ . '/../../';

    /** @var MySQL */
    protected $writer;

    /** @var array */
    protected $config;

    /** @var string */
    protected $dataDir = __DIR__ . '/../../tests/data';

    /** @var string */
    protected $tmpDataDir = '/tmp/wr-db/data';

    /** @var string */
    protected $appName = 'wr-db-mysql-tests';

    public function initFixtures(array $config, ?string $sourceDataDir = null): void
    {
        $dataDir = $sourceDataDir ?: $this->dataDir;

        $fs = new Filesystem();
        if ($fs->exists($this->tmpDataDir)) {
            $fs->remove($this->tmpDataDir);
        }
        $fs->mkdir($this->tmpDataDir);
        $fs->dumpFile($this->tmpDataDir . '/config.json', json_encode($config));
        if ($fs->exists($dataDir . '/in/tables')) {
            $fs->mirror($dataDir . '/in/tables', $this->tmpDataDir . '/in/tables');
        }
    }

    protected function runProcess(): Process
    {
        $process = new Process(sprintf('php %srun.php --data=%s', $this->rootDir, $this->tmpDataDir));
        $process->setTimeout(300);
        $process->run();

        return $process;
    }

    protected function cleanup(array $config): void
    {
        $writer = $this->getWriter($config['parameters']);
        if (isset($config['parameters']['tables'])) {
            $tables = $config['parameters']['tables'];
            foreach ($tables as $table) {
                $writer->drop($table['dbName']);
            }
        } elseif (isset($config['parameters']['dbName'])) {
            $writer->drop($config['parameters']['dbName']);
        }
    }

    protected function getConfig(?string $dataDir = null): array
    {
        $dataDir = $dataDir ?: $this->dataDir;
        $config = json_decode(file_get_contents($dataDir . '/config.json'), true);
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['db']['user'] = $this->getEnv('DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv('DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv('DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv('DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv('DB_DATABASE');
        $config['parameters']['writer_class'] = 'MySQL';

        return $config;
    }
}
