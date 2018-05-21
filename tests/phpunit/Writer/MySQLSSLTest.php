<?php

namespace Keboola\DbWriter\Tests\Writer;

use Keboola\DbWriter\Application;
use Keboola\DbWriter\Configuration\MySQLConfigDefinition;
use Keboola\DbWriter\Test\BaseTest;
use Keboola\DbWriter\Logger;

class MySQLSSLTest extends BaseTest
{
    private $config;

    protected $dataDir = __DIR__ . '../../data';

    /** @var \PDO */
    protected $pdo;

    public function setUp()
    {
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::MYSQL_ATTR_LOCAL_INFILE => true
        ];

        $options[\PDO::MYSQL_ATTR_SSL_KEY] = realpath($this->dataDir . '/mysql/ssl/client-key.pem');
        $options[\PDO::MYSQL_ATTR_SSL_CERT] = realpath($this->dataDir . '/mysql/ssl/client-cert.pem');
        $options[\PDO::MYSQL_ATTR_SSL_CA] = realpath($this->dataDir . '/mysql/ssl/ca.pem');

        $config = $this->getConfig();
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
        $this->config = $this->getConfig();

        $this->config['parameters']['writer_class'] = 'MySQL';
        $this->config['action'] = 'testConnection';

        $this->config['parameters']['db']['ssl'] = [
            'enabled' => true,
            'ca' => file_get_contents($this->dataDir . '/mysql/ssl/ca.pem'),
            'cert' => file_get_contents($this->dataDir . '/mysql/ssl/client-cert.pem'),
            'key' => file_get_contents($this->dataDir . '/mysql/ssl/client-key.pem'),
        ];

        unset($this->config['parameters']['tables']);


        $app = new Application($this->config, new Logger(APP_NAME), new MySQLConfigDefinition());

        $response = $app->run();

        $this->assertContains('success', $response);
    }
}
