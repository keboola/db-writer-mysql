<?php

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Test\MySQLBaseTest;

class EntrypointTest extends MySQLBaseTest
{
    public function testRunAction()
    {
        $config = $this->getConfig($this->dataDir . '/runAction');
        $this->cleanup($config);
        $this->initFixtures($config, $this->dataDir . '/runAction');

        $process = $this->runProcess();
        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());
    }

    public function testConnectionAction()
    {
        $config = $this->getConfig($this->dataDir . '/connectionAction');
        $this->initFixtures($config, $this->dataDir . '/connectionAction');

        $process = $this->runProcess();
        $this->assertEquals(0, $process->getExitCode());

        $data = json_decode($process->getOutput(), true);
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }
}
