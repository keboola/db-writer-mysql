<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Test\MySQLBaseTest;
use Keboola\DbWriter\Writer\MySQL;
use Keboola\DbWriter\WriterFactory;
use Monolog\Handler\TestHandler;

class MySQLSSHTest extends MySQLBaseTest
{
    /** @var MySQL */
    private $writer;

    /** @var array */
    private $config;

    /** @var TestHandler */
    private $testHandler;

    public function setUp(): void
    {
        var_dump($this->getPrivateKey());
        var_dump($this->getPublicKey());
        $this->config = $this->getConfig();
        $this->config['parameters']['writer_class'] = 'MySQL';
        $this->config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
            'localPort' => '23306',
        ];

        $this->testHandler = new TestHandler();

        $logger = new Logger('wr-db-mysql-tests');
        $logger->setHandlers([$this->testHandler]);

        $writerFactory = new WriterFactory($this->config['parameters']);
        $this->writer = $writerFactory->create($logger);
    }

    public function getPrivateKey(): string
    {
        return file_get_contents('/root/.ssh/id_rsa');
    }
    public function getPublicKey(): string
    {
        return file_get_contents('/root/.ssh/id_rsa.pub');
    }

    public function testWriteMysql(): void
    {
        $tables = $this->config['parameters']['tables'];

        // simple table
        $table = $tables[0];
        $sourceTableId = $table['tableId'];
        $outputTableName = $table['dbName'];
        $sourceFilename = $this->dataDir . '/' . $sourceTableId . ".csv";

        $this->writer->drop($outputTableName);
        $this->writer->create($table);
        $this->writer->write(new CsvFile(realpath($sourceFilename)), $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM $outputTableName");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id","name","glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($sourceFilename, $resFilename);

        // table with special chars
        $table = $tables[1];
        $sourceTableId = $table['tableId'];
        $outputTableName = $table['dbName'];
        $sourceFilename = $this->dataDir . '/' . $sourceTableId . ".csv";

        $this->writer->drop($outputTableName);
        $this->writer->create($table);
        $this->writer->write(new CsvFile(realpath($sourceFilename)), $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM $outputTableName");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp-2');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["col1","col2"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($sourceFilename, $resFilename);

        // ignored columns
        $table = $tables[0];
        $sourceTableId = $table['tableId'];
        $outputTableName = $table['dbName'];
        $sourceFilename = $this->dataDir . '/' . $sourceTableId . ".csv";

        $table['items'][2]['type'] = 'IGNORE';

        $this->writer->drop($outputTableName);
        $this->writer->create($table);
        $this->writer->write(new CsvFile(realpath($sourceFilename)), $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM $outputTableName");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resArr = [];
        foreach ($res as $row) {
            $resArr[] = array_values($row);
        }

        $srcArr = [];
        $csv = new CsvFile($sourceFilename);
        $csv->next();
        $csv->next();

        while ($csv->current()) {
            $currRow = $csv->current();
            unset($currRow[2]);
            $srcArr[] = array_values($currRow);
            $csv->next();
        }

        $this->assertEquals($srcArr, $resArr);

        $records = $this->testHandler->getRecords();

        $this->assertCount(14, $records);

        $this->assertArrayHasKey('message', $records[0]);
        $this->assertArrayHasKey('level', $records[0]);
        $this->assertArrayHasKey('message', $records[1]);
        $this->assertArrayHasKey('level', $records[1]);

        $this->assertEquals(Logger::INFO, $records[0]['level']);
        $this->assertRegExp('/Creating SSH tunnel/ui', $records[0]['message']);

        $this->assertEquals(Logger::INFO, $records[1]['level']);
        $this->assertRegExp('/Connecting to DSN/ui', $records[1]['message']);
    }
}
