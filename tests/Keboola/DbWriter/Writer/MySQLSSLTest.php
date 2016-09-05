<?php
namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Application;
use Keboola\DbWriter\Configuration\MySQLConfigDefinition;
use Keboola\DbWriter\Test\BaseTest;

class MySQLSSLTest extends BaseTest
{
	const DRIVER = 'mysql';

	private $config;

	/**
	 * @var \PDO
	 */
	protected $pdo;

	public function setUp()
	{
		if (!defined('APP_NAME')) {
			define('APP_NAME', 'wr-db-mysql');
		}

		$options = [
			\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
			\PDO::MYSQL_ATTR_LOCAL_INFILE => true
		];

		$options[\PDO::MYSQL_ATTR_SSL_KEY] = realpath($this->dataDir . '/mysql/ssl/client-key.pem');
		$options[\PDO::MYSQL_ATTR_SSL_CERT] = realpath($this->dataDir . '/mysql/ssl/client-cert.pem');
		$options[\PDO::MYSQL_ATTR_SSL_CA] = realpath($this->dataDir . '/mysql/ssl/ca.pem');

		$config = $this->getConfig('mysql');
		$dbConfig = $config['parameters']['db'];

		$dsn = sprintf(
			"mysql:host=%s;port=%s;dbname=%s;charset=utf8",
			$dbConfig['host'],
			$dbConfig['port'],
			$dbConfig['database']
		);

		$this->pdo = new \PDO($dsn, $dbConfig['user'], $dbConfig['#password'], $options);

		$this->pdo->setAttribute(\PDO::MYSQL_ATTR_LOCAL_INFILE, true);
		$this->pdo->exec("SET NAMES utf8;");
	}

	public function testSSLEnabled()
	{
		$status = $this->pdo->query("SHOW STATUS LIKE 'Ssl_cipher';")->fetch(\PDO::FETCH_ASSOC);

		$this->assertArrayHasKey('Value', $status);
		$this->assertNotEmpty($status['Value']);
	}

	public function testCredentials()
	{
		$this->config = $this->getConfig(self::DRIVER);

		$this->config['parameters']['writer_class'] = 'MySQL';
		$this->config['action'] = 'testConnection';

		$this->config['parameters']['db']['ssl'] = [
			'enabled' => true,
			'ca' => file_get_contents($this->dataDir . '/mysql/ssl/ca.pem'),
			'cert' => file_get_contents($this->dataDir . '/mysql/ssl/client-cert.pem'),
			'key' => file_get_contents($this->dataDir . '/mysql/ssl/client-key.pem'),
//			'cipher' => '',
		];

		unset($this->config['parameters']['tables']);


		$app = new Application($this->config);
		$app->setConfigDefinition(new MySQLConfigDefinition());

		$response = $app->run();

		$this->assertArrayHasKey('status', $response);
		$this->assertEquals('success', $response['status']);
	}
}
