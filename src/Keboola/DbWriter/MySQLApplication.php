<?php
namespace Keboola\DbWriter;

use Keboola\DbWriter\Configuration\MySQLConfigDefinition;

class MySQLApplication extends Application
{
	public function __construct(array $config, Logger $logger)
	{
		parent::__construct($config, $logger);

		$this->setConfigDefinition(new MySQLConfigDefinition());
	}
}