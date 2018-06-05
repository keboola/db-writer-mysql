<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 05/11/15
 * Time: 13:33
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Test\BaseTest;

class MySQLTest extends BaseTest
{
	const DRIVER = 'mysql';

	/** @var MySQL */
	private $writer;

	private $config;

	public function setUp()
	{
		if (!defined('APP_NAME')) {
			define('APP_NAME', 'wr-db-mysql');
		}

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

    public function testCreateDefaultValue()
    {
        $tables = $this->config['parameters']['tables'];
        $tables[0]['items'][2]['nullable'] = true;
        $tables[0]['items'][2]['default'] = 'dioptric.big';

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
		$sourceFilename = $this->dataDir . "/mysql/" . $sourceTableId . ".csv";

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
		$sourceFilename = $this->dataDir . "/mysql/" . $sourceTableId . ".csv";

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
		$csv->next();$csv->next();

		while ($csv->current()) {
			$currRow = $csv->current();
			unset($currRow[2]);
			$srcArr[] = array_values($currRow);
			$csv->next();
		}

		$this->assertEquals($srcArr, $resArr);
	}

    public function testWriteDefaultValue()
    {
        $tables = $this->config['parameters']['tables'];

        // simple table
        $table = $tables[0];
        $sourceTableId = $table['tableId'];
        $outputTableName = $table['dbName'];
        $sourceFilename = $this->dataDir . "/mysql/" . $sourceTableId . "_default.csv";

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

        $expected = $this->dataDir . "/mysql/" . $sourceTableId . ".csv";

        $this->assertFileEquals($expected, $resFilename);
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
		$this->writer->write(new CsvFile($sourceFilename), $targetTable);

		// second write
		$sourceFilename = $this->dataDir . "/mysql/" . $table['tableId'] . "_increment.csv";
		$this->writer->create($table);
		$this->writer->write(new CsvFile($sourceFilename), $table);
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

    public function testUpsertWithoutPK()
    {
        $conn = $this->writer->getConnection();
        $tables = $this->config['parameters']['tables'];

        $table = $tables[0];
        $table['primaryKey'] = [];

        $sourceFilename = $this->dataDir . "/mysql/" . $table['tableId'] . ".csv";
        $targetTable = $table;
        $table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';

        // first write
        $this->writer->create($targetTable);
        $this->writer->write(new CsvFile($sourceFilename), $targetTable);

        // second write
        $sourceFilename = $this->dataDir . "/mysql/" . $table['tableId'] . "_increment.csv";
        $this->writer->create($table);
        $this->writer->write(new CsvFile($sourceFilename), $table);
        $this->writer->upsert($table, $targetTable['dbName']);

        $stmt = $conn->query("SELECT * FROM {$targetTable['dbName']}");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id", "name", "glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->dataDir . "/mysql/" . $table['tableId'] . "_merged_no_pk.csv";

        $this->assertFileEquals($expectedFilename, $resFilename);
    }

	public function testReorderColumns()
	{
		$conn = $this->writer->getConnection();
		$tables = $this->config['parameters']['tables'];

		$table = $tables[0];
		$table['items'] = array_reverse($table['items']);

		$sourceFilename = $this->dataDir . "/mysql/" . $table['tableId'] . ".csv";
		$targetTable = $table;
		$table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';

		// first write
		$this->writer->create($targetTable);
		$this->writer->write(new CsvFile($sourceFilename), $targetTable);

		// second write
		$sourceFilename = $this->dataDir . "/mysql/" . $table['tableId'] . "_increment.csv";

		$this->writer->create($table);
		$this->writer->write(new CsvFile($sourceFilename), $table);
		$this->writer->upsert($table, $targetTable['dbName']);


		$expectedFilename = $this->dataDir . "/mysql/" . $table['tableId'] . "_merged.csv";
		$expectedFile = new CsvFile($expectedFilename);
		$header = $expectedFile->getHeader();


		$stmt = $conn->query("SELECT " . implode(', ', $header) . " FROM {$targetTable['dbName']}");
		$res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

		$resFilename = tempnam('/tmp', 'db-wr-test-tmp');
		$csv = new CsvFile($resFilename);
		$csv->writeRow($header);
		foreach ($res as $row) {
			$csv->writeRow($row);
		}

		$this->assertFileEquals($expectedFilename, $resFilename);
	}


	public function testReorderRenameIgnoreColumns()
	{
		$conn = $this->writer->getConnection();
		$tables = $this->config['parameters']['tables'];

		$table = $tables[0];

		// reorder
		$table['items'] = array_reverse($table['items']);

		// rename
		foreach ($table['items'] AS $key => $column) {
			$table['items'][$key]['dbName'] = md5($column['dbName']);
		}
		foreach ($table['primaryKey'] AS $key => $column) {
			$table['primaryKey'][$key] = md5($column);
		}

		// ignore
		foreach ($table['items'] AS $key => $column) {
			if ($column['name'] === 'glasses') {
				$table['items'][$key]['type'] = 'IGNORE';
			}
		}

		$sourceFilename = $this->dataDir . "/mysql/" . $table['tableId'] . ".csv";
		$targetTable = $table;
		$table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';

		// first write
		$this->writer->create($targetTable);
		$this->writer->write(new CsvFile($sourceFilename), $targetTable);

		// second write
		$sourceFilename = $this->dataDir . "/mysql/" . $table['tableId'] . "_increment.csv";

		$this->writer->create($table);
		$this->writer->write(new CsvFile($sourceFilename), $table);
		$this->writer->upsert($table, $targetTable['dbName']);


		$expectedFilename = $this->dataDir . "/mysql/" . $table['tableId'] . "_merged.csv";
		$expectedCsv = new CsvFile($expectedFilename);

		// prepare validation file
		$expectedHeaderMap = array();
		foreach ($table['items'] AS $column) {
			if ($column['type'] === 'IGNORE') {
				continue;
			}

			$expectedHeaderMap[$column['name']] = $column['dbName'];
		}

		$tmpExpectedFilename = tempnam('/tmp', md5($expectedFilename));
		$tmpExpectedCsv = new CsvFile($tmpExpectedFilename);

		$header = $expectedCsv->getHeader();
		foreach ($expectedCsv AS $i => $row) {
			if (!$i) {
				$tmpExpectedCsv->writeRow($expectedHeaderMap);
				continue;
			}

			$newRow = [];

			$row = array_combine($header, $row);
			foreach ($expectedHeaderMap AS $originName => $newName) {
				$newRow[$newName] = $row[$originName];
			}

			$tmpExpectedCsv->writeRow($newRow);
		}

		$stmt = $conn->query("SELECT " . implode(', ', $expectedHeaderMap) . " FROM {$targetTable['dbName']}");
		$res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

		$resFilename = tempnam('/tmp', 'db-wr-test-tmp');
		$csv = new CsvFile($resFilename);
		$csv->writeRow($expectedHeaderMap);
		foreach ($res as $row) {
			$csv->writeRow($row);
		}

		$this->assertFileEquals($tmpExpectedFilename, $resFilename);
	}


	public function testGenerateTmpName()
	{
		$tables = $this->config['parameters']['tables'];

		$table = $tables[0];

		$tableName = 'firstTable';

		$tmpName = $this->writer->generateTmpName($tableName);
		$this->assertRegExp('/' . $tableName . '/ui', $tmpName);
		$this->assertRegExp('/temp/ui', $tmpName);
		$this->assertLessThanOrEqual(64, mb_strlen($tmpName));

		$table['dbName'] = $tableName;
		$this->writer->drop($tableName);
		$this->writer->create($table);

		$tableName = str_repeat('firstTableWithLongName', 6);

		$tmpName = $this->writer->generateTmpName($tableName);
		$this->assertRegExp('/temp/ui', $tmpName);
		$this->assertLessThanOrEqual(64, mb_strlen($tmpName));

		$table['dbName'] = $tmpName;
		$this->writer->drop($tmpName);
		$this->writer->create($table);
	}

	public function testCheckKeysOK()
    {
        $tableConfig = $this->config['parameters']['tables'][0];
        $this->writer->create($tableConfig);
        $this->writer->checkKeys($tableConfig['primaryKey'], $tableConfig['dbName']);

        // no exception thrown, that's good
        $this->assertTrue(true);
    }

    public function testCheckKeysError()
    {
        $this->setExpectedException(
            get_class(new UserException()),
            "Primary key(s) in configuration does NOT match with keys in DB table."
        );
        $tableConfig = $this->config['parameters']['tables'][0];
        $tableConfigWithOtherPrimaryKeys = $tableConfig;
        $tableConfigWithOtherPrimaryKeys['items'][0]['dbName'] = 'code';
        $tableConfigWithOtherPrimaryKeys['primaryKey'] = ['code'];
        $this->writer->create($tableConfigWithOtherPrimaryKeys);
        $this->writer->checkKeys($tableConfig['primaryKey'], $tableConfig['dbName']);
    }
}
