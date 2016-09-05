<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 05/11/15
 * Time: 13:33
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Test\BaseTest;

class MySQLTest extends BaseTest
{
	const DRIVER = 'mysql';

	/** @var MySQL */
	private $writer;

	private $config;

	public function setUp()
	{
		$this->config = $this->getConfig(self::DRIVER);
		$this->config['parameters']['writer_class'] = 'MySQL';
		$this->writer = $this->getWriter($this->config['parameters']);
		$conn = $this->writer->getConnection();

		$tables = $this->config['parameters']['tables'];

		foreach ($tables as $table) {
			$conn->exec(sprintf('DROP TABLE IF EXISTS %s', $table['dbName']));
		}
	}

	public function testDrop()
	{
		$conn = $this->writer->getConnection();
		$this->writer->drop("dropMe");

		$conn->exec("CREATE TABLE dropMe (
          id INT PRIMARY KEY,
          firstname VARCHAR(30) NOT NULL,
          lastname VARCHAR(30) NOT NULL)");

		$this->writer->drop("dropMe");

		$stmt = $conn->query("SELECT Distinct TABLE_NAME FROM information_schema.TABLES");
		$res = $stmt->fetchAll();

		$tableExists = false;
		foreach ($res as $r) {
			if ($r[0] == "dropMe") {
				$tableExists = true;
				break;
			}
		}

		$this->assertFalse($tableExists);
	}

	public function testCreate()
	{
		$tables = $this->config['parameters']['tables'];

		foreach ($tables as $table) {
			$this->writer->create($table);
		}

		/** @var \PDO $conn */
		$conn = $this->writer->getConnection();
		$stmt = $conn->query("SELECT Distinct TABLE_NAME FROM information_schema.TABLES");
		$res = $stmt->fetchAll();

		$tableExits = false;
		foreach ($res as $r) {
			if ($r['TABLE_NAME'] == $tables[0]['dbName']) {
				$tableExits = true;
				break;
			}
		}

		$this->assertTrue($tableExits);
	}

	public function testWriteMysql()
	{
		$tables = $this->config['parameters']['tables'];

		// simple table
		$table = $tables[0];
		$sourceTableId = $table['tableId'];
		$outputTableName = $table['dbName'];
		$sourceFilename = $this->dataDir . "/mysql/" . $sourceTableId . ".csv";

		$this->writer->drop($outputTableName);
		$this->writer->create($table);
		$this->writer->write(realpath($sourceFilename), $table);

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
		$sourceFilename = $this->dataDir . "/mysql/" . $sourceTableId . ".csv";

		$this->writer->drop($outputTableName);
		$this->writer->create($table);
		$this->writer->write(realpath($sourceFilename), $table);

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
		$sourceFilename = $this->dataDir . "/mysql/" . $sourceTableId . ".csv";

		$table['items'][2]['type'] = 'IGNORE';

		$this->writer->drop($outputTableName);
		$this->writer->create($table);
		$this->writer->write(realpath($sourceFilename), $table);

		$conn = $this->writer->getConnection();
		$stmt = $conn->query("SELECT * FROM $outputTableName");
		$res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

		$resArr = [];
		foreach ($res as $row) {
			$resArr[] = array_values($row);
		}

		$srcArr = [];
		$csv = new CsvFile($sourceFilename);
		$csv->next();$csv->next();

		while ($csv->current()) {
			$currRow = $csv->current();
			unset($currRow[2]);
			$srcArr[] = array_values($currRow);
			$csv->next();
		}

		$this->assertEquals($srcArr, $resArr);
	}

	public function testGetAllowedTypes()
	{
		$allowedTypes = $this->writer->getAllowedTypes();

		$this->assertEquals([
			'int', 'smallint', 'bigint',
			'decimal', 'float', 'double',
			'date', 'datetime', 'timestamp',
			'char', 'varchar', 'text', 'blob'
		], $allowedTypes);
	}

	public function testUpsert()
	{
		$conn = $this->writer->getConnection();
		$tables = $this->config['parameters']['tables'];

		$table = $tables[0];
		$sourceFilename = $this->dataDir . "/mysql/" . $table['tableId'] . ".csv";
		$targetTable = $table;
		$table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';

		// first write
		$this->writer->create($targetTable);
		$this->writer->write($sourceFilename, $targetTable);

		// second write
		$sourceFilename = $this->dataDir . "/mysql/" . $table['tableId'] . "_increment.csv";
		$this->writer->create($table);
		$this->writer->write($sourceFilename, $table);
		$this->writer->upsert($table, $targetTable['dbName']);

		$stmt = $conn->query("SELECT * FROM {$targetTable['dbName']}");
		$res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

		$resFilename = tempnam('/tmp', 'db-wr-test-tmp');
		$csv = new CsvFile($resFilename);
		$csv->writeRow(["id", "name", "glasses"]);
		foreach ($res as $row) {
			$csv->writeRow($row);
		}

		$expectedFilename = $this->dataDir . "/mysql/" . $table['tableId'] . "_merged.csv";

		$this->assertFileEquals($expectedFilename, $resFilename);
	}

//    public function testExecutor()
//    {
//        $config = $this->getConfig(self::DRIVER);
//        $tables = $config['parameters']['tables'];
//        $outputTableName = $tables[0]['dbName'];
//        $sourceTableId = $tables[0]['tableId'];
//        $sourceFilename = $this->dataDir . "/" . self::DRIVER . "/in/tables/" . $sourceTableId . ".csv";
//
//        $executor = $this->getExecutor(self::DRIVER);
//        $executor->run();
//
//        $conn = $this->getWriter(self::DRIVER)->getConnection();
//        $stmt = $conn->query("SELECT * FROM $outputTableName");
//        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);
//
//        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
//        $csv = new CsvFile($resFilename);
//        $csv->writeRow(["id","name","hasGlasses","double"]);
//        foreach ($res as $row) {
//            $csv->writeRow($row);
//        }
//
//        $this->assertFileEquals($sourceFilename, $resFilename);
//    }
}
