<?php
namespace Keboola\DbWriter;

use Keboola\DbWriter\Configuration\MySQLConfigDefinition;
use Psr\Log\LoggerInterface;

class MySQLApplication extends Application
{
	public function __construct(array $config, LoggerInterface $logger)
	{
		parent::__construct($config);

		$this['logger'] = function() use ($logger) {
			return $logger;
		};

		$this->setConfigDefinition(new MySQLConfigDefinition());
	}
}