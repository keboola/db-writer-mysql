<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Configuration\MySQLConfigDefinition;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\MySQLApplication;
use Keboola\DbWriter\Test\MySQLBaseTest;
use PDO;

class SSLTest extends MySQLBaseTest
{
    /** @var PDO $pdo */
    protected $pdo;

    public function setUp(): void
    {
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::MYSQL_ATTR_LOCAL_INFILE => true,
            PDO::MYSQL_ATTR_SSL_KEY => '/ssl-cert/client-key.pem',
            PDO::MYSQL_ATTR_SSL_CERT => '/ssl-cert/client-cert.pem',
            PDO::MYSQL_ATTR_SSL_CA => '/ssl-cert/ca-cert.pem',
        ];

        $config = $this->getConfig();
        $dbConfig = $config['parameters']['db'];
        $dsn = sprintf(
            'mysql:host=%s;port=%s;dbname=%s;charset=utf8',
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['database']
        );

        $this->pdo = new PDO($dsn, $dbConfig['user'], $dbConfig['#password'], $options);
        $this->pdo->setAttribute(PDO::MYSQL_ATTR_LOCAL_INFILE, true);
        $this->pdo->exec('SET NAMES utf8;');
    }

    public function testSSLEnabled(): void
    {
        $status = $this->pdo->query("SHOW STATUS LIKE 'Ssl_cipher';")->fetch(PDO::FETCH_ASSOC);

        $this->assertArrayHasKey('Value', $status);
        $this->assertNotEmpty($status['Value']);
    }

    public function testCredentials(): void
    {
        $config = $this->getConfig();
        $config['parameters']['writer_class'] = 'MySQL';
        $config['action'] = 'testConnection';
        $config['parameters']['db']['ssl'] = [
            'enabled' => true,
            'ca' => file_get_contents('/ssl-cert/ca-cert.pem'),
            'cert' => file_get_contents('/ssl-cert/client-cert.pem'),
            '#key' => file_get_contents('/ssl-cert/client-key.pem'),
        ];
        unset($config['parameters']['tables']);

        $app = new MySQLApplication($config, new Logger($this->appName));

        $response = $app->run();

        $this->assertContains('success', $response);
    }

    protected function getConfig(?string $dataDir = null): array
    {
        $config = parent::getConfig($dataDir);
        $config['parameters']['db']['host'] = (string) getenv('DB_SSL_HOST');
        return $config;
    }
}
