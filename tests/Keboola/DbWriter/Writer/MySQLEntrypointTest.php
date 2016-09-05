<?php
namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Yaml\Yaml;

class MySQLEntrypointTest extends BaseTest
{
	const DRIVER = 'mysql';

	/** @var MySQL */
	private $writer;

	private $config;

	public function testRunAction()
	{
		$rootPath = __DIR__ . '/../../../../';

		$this->config = Yaml::parse(file_get_contents($rootPath . 'tests/data/runAction/config.yml'));
		$this->config['parameters']['writer_class'] = 'MySQL';

		$this->writer = $this->getWriter($this->config['parameters']);

		// cleanup
		foreach ($this->config['parameters']['tables'] AS $table) {
			$this->writer->drop($table['dbName']);
		}

		// run entrypoint
		$lastOutput = exec('php ' . $rootPath . 'run.php --data=' . $rootPath . 'tests/data/runAction 2>&1', $output, $returnCode);

		$this->assertEquals(0, $returnCode);

	}

	public function testConnectionAction()
	{
		$rootPath = __DIR__ . '/../../../../';

		$lastOutput = exec('php ' . $rootPath . 'run.php --data=' . $rootPath . 'tests/data/connectionAction 2>&1', $output, $returnCode);

		$this->assertEquals(0, $returnCode);

		$data = json_decode($lastOutput, true);
		$this->assertArrayHasKey('status', $data);
		$this->assertEquals('success', $data['status']);
	}
}
