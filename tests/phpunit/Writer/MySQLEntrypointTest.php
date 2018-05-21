<?php

namespace Keboola\DbWriter\Tests\Writer;

use Keboola\DbWriter\Test\BaseTest;
use Keboola\DbWriter\Writer\MySQL;
use Symfony\Component\Process\Process;

class MySQLEntrypointTest extends BaseTest
{
    const DRIVER = 'mysql';

    /** @var MySQL */
    private $writer;

    private $config;

    public function testRunAction()
    {
        $rootPath = __DIR__ . '/../../../';

        $this->config = json_decode(file_get_contents($rootPath . 'tests/data/runAction/config.json'), true);
        $this->config['parameters']['writer_class'] = 'MySQL';

        $this->writer = $this->getWriter($this->config['parameters']);

        // cleanup
        foreach ($this->config['parameters']['tables'] as $table) {
            $this->writer->drop($table['dbName']);
        }

        // run entrypoint
        $process = new Process('php ' . $rootPath . 'run.php --data=' . $rootPath . 'tests/data/runAction 2>&1');
        $exitCode = $process->run();

        $this->assertEquals(0, $exitCode, $process->getOutput());
    }

    public function testConnectionAction()
    {
        $rootPath = __DIR__ . '/../../../db-writer-mysql/';

        $lastOutput = exec('php ' . $rootPath . 'run.php --data=' . $rootPath . 'tests/data/connectionAction 2>&1', $output, $returnCode);

        $this->assertEquals(0, $returnCode);

        $this->assertCount(1, $output);
        $this->assertEquals($lastOutput, reset($output));

        $data = json_decode($lastOutput, true);
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }
}
