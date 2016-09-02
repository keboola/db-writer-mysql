<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 12/02/16
 * Time: 16:38
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;

class MySQL extends Writer implements WriterInterface
{
	private static $allowedTypes = [
		'int', 'smallint', 'bigint',
		'decimal', 'float', 'double',
		'date', 'datetime', 'timestamp',
		'char', 'varchar', 'text', 'blob'
	];


	private static $typesWithSize = [
		'decimal', 'float',
		'datetime', 'time',
		'char', 'varchar',
	];

	private static $unicodeTypes = [
		'nchar', 'nvarchar', 'ntext',
	];

	private static $numericTypes = [
		'int', 'smallint', 'bigint',
		'decimal', 'float'
	];

	/** @var \PDO */
	protected $db;

	private $batched = true;

	public function createConnection($dbParams)
	{
		$isSsl = false;

		// convert errors to PDOExceptions
		$options = [
			\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
			\PDO::MYSQL_ATTR_LOCAL_INFILE => true
		];

		if (!empty($dbParams['batched'])) {
			if ($dbParams['batched'] == false) {
				$this->batched = false;
			}
		}

		// check params
		foreach (['host', 'database', 'user', '#password'] as $r) {
			if (!isset($dbParams[$r])) {
				throw new UserException(sprintf("Parameter %s is missing.", $r));
			}
		}

		// ssl encryption
		if (!empty($params['ssl']) && !empty($params['ssl']['enabled'])) {
			$ssl = $params['ssl'];

			$temp = new Temp(defined('APP_NAME') ? APP_NAME : 'wr-db-mysql');

			if (!empty($ssl['key'])) {
				$options[\PDO::MYSQL_ATTR_SSL_KEY] = $this->createSSLFile($ssl['key'], $temp);
				$isSsl = true;
			}
			if (!empty($ssl['cert'])) {
				$options[\PDO::MYSQL_ATTR_SSL_CERT] = $this->createSSLFile($ssl['cert'], $temp);
				$isSsl = true;
			}
			if (!empty($ssl['ca'])) {
				$options[\PDO::MYSQL_ATTR_SSL_CA] = $this->createSSLFile($ssl['ca'], $temp);
				$isSsl = true;
			}
			if (!empty($ssl['cipher'])) {
				$options[\PDO::MYSQL_ATTR_SSL_CIPHER] = $ssl['cipher'];
			}
		}

		$port = isset($dbParams['port']) ? $dbParams['port'] : '3306';

		$dsn = sprintf(
			"mysql:host=%s;port=%s;dbname=%s;charset=utf8",
			$dbParams['host'],
			$port,
			$dbParams['database']
		);

		$pdo = new \PDO($dsn, $dbParams['user'], $dbParams['#password'], $options);
		$pdo->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
		$pdo->exec("SET NAMES utf8;");


		return $pdo;
	}

	function write($sourceFilename, array $table)
	{
		$query = "
            LOAD DATA LOCAL INFILE '{$sourceFilename}'
            INTO TABLE {$this->escape($table['dbName'])}
            CHARACTER SET utf8
            FIELDS TERMINATED BY ','
            OPTIONALLY ENCLOSED BY '\"'
            ESCAPED BY ''
            IGNORE 1 LINES
        ";

		try {
			$this->db->exec($query);
		} catch (\PDOException $e) {
			throw $e;
			throw new UserException("Query failed: " . $e->getMessage(), $e, [
				'query' => $query
			]);
		}
return;
		$csv = new CsvFile($sourceFilename);

		// skip the header
		$csv->next();
		$csv->next();

		$columnsCount = count($csv->current());
		$rowsPerInsert = intval((1000 / $columnsCount) - 1);

		$this->db->beginTransaction();

		while ($csv->current() !== false) {

			$sql = "INSERT INTO " . $this->escape($table['dbName']) . " VALUES ";

			for ($i=0; $i<1 && $csv->current() !== false; $i++) {
				$sql .= sprintf(
					"(%s),",
					implode(
						',',
						$this->encodeCsvRow(
							$this->escapeCsvRow($csv->current()),
							$table['items']
						)
					)
				);
				$csv->next();
			}
			$sql = substr($sql, 0, -1);

			echo sprintf("Executing query '%s'.", $sql);
			echo PHP_EOL;
			$this->db->exec($sql);
		}

		$this->db->commit();
	}

	private function encodeCsvRow($row, $columnDefinitions)
	{
		$res = [];
		foreach ($row as $k => $v) {
			if (strtolower($columnDefinitions[$k]['type']) == 'ignore') {
				continue;
			}
			$decider = $this->getEncodingDecider($columnDefinitions[$k]['type']);
			$res[$k] = $decider($v);
		}

		return $res;
	}

	private function getEncodingDecider($type)
	{
		return function ($data) use ($type) {
			if (strtolower($data) === 'null') {
				return $data;
			}

			if (in_array(strtolower($type), self::$numericTypes) && empty($data)) {
				return 0;
			}

			if (!in_array(strtolower($type), self::$numericTypes)) {
				$data = "'" . $data . "'";
			}

			if (in_array(strtolower($type), self::$unicodeTypes)) {
				return "N" . $data;
			}

			return $data;
		};
	}

	private function escapeCsvRow($row)
	{
		$res = [];
		foreach ($row as $k => $v) {
			$res[$k] = $this->msEscapeString($v);
		}

		return $res;
	}

	private function msEscapeString($data) {
		if ( !isset($data) or empty($data) ) return '';
		if ( is_numeric($data) ) return $data;

		$non_displayables = array(
			'/%0[0-8bcef]/',            // url encoded 00-08, 11, 12, 14, 15
			'/%1[0-9a-f]/',             // url encoded 16-31
			'/[\x00-\x08]/',            // 00-08
			'/\x0b/',                   // 11
			'/\x0c/',                   // 12
			'/[\x0e-\x1f]/'             // 14-31
		);
		foreach ( $non_displayables as $regex ) {
			$data = preg_replace( $regex, '', $data );
		}
		$data = str_replace("'", "''", $data );
		return $data;
	}

	function isTableValid(array $table, $ignoreExport = false)
	{
		// TODO: Implement isTableValid() method.

		return true;
	}

	function drop($tableName)
	{
		$this->db->exec("DROP TABLE IF EXISTS `{$tableName}`;");
	}

	private function escape($obj)
	{
		$objNameArr = explode('.', $obj);

		if (count($objNameArr) > 1) {
			return $objNameArr[0] . ".`" . $objNameArr[1] . "`";
		}

		return "`" . $objNameArr[0] . "`";
	}

	function create(array $table)
	{
		$sql = "CREATE TABLE `{$table['dbName']}` (";

		$columns = $table['items'];
		foreach ($columns as $k => $col) {

			$type = strtoupper($col['type']);
			if ($type == 'IGNORE') {
				continue;
			}

			if (!empty($col['size'])) {
				$type .= "({$col['size']})";
			}

			$null = $col['nullable'] ? 'NULL' : 'NOT NULL';

			$default = empty($col['default']) ? '' : $col['default'];
			if ($type == 'TEXT') {
				$default = '';
			}

			$sql .= "`{$col['dbName']}` $type $null $default";
			$sql .= ',';
		}

		$sql = substr($sql, 0, -1);
		$sql .= ") DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";

		$this->db->exec($sql);
		return;
		$sql = "create table {$this->escape($table['dbName'])} (";

		$columns = $table['items'];
		foreach ($columns as $k => $col) {

			$type = strtolower($col['type']);
			if ($type == 'ignore') {
				continue;
			}

			if (!empty($col['size']) && in_array($type, self::$typesWithSize)) {
				$type .= "({$col['size']})";
			}

			$null = empty($col['nullable']) ? 'NULL' : 'NOT NULL';

			$default = empty($col['default']) ? '' : $col['default'];
			if ($type == 'text') {
				$default = '';
			}

			$sql .= "{$this->escape($col['dbName'])} $type $null $default";
			$sql .= ',';
		}

		$sql = substr($sql, 0, -1);
		$sql .= ");";

		$this->db->exec($sql);
	}

	static function getAllowedTypes()
	{
		return self::$allowedTypes;
	}

	public function upsert(array $table, $targetTable)
	{
		$sourceTable = $this->escape($table['dbName']);
		$targetTable = $this->escape($targetTable);

		$columns = array_map(function($item) {
			return $this->escape($item['dbName']);
		}, $table['items']);

		if (!empty($table['primaryKey'])) {
			// update data
			$joinClauseArr = [];
			foreach ($table['primaryKey'] as $index => $value) {
				$joinClauseArr[] = "a.{$value}=b.{$value}";
			}
			$joinClause = implode(' AND ', $joinClauseArr);

			$valuesClauseArr = [];
			foreach ($columns as $index => $column) {
				$valuesClauseArr[] = "a.{$column}=b.{$column}";
			}
			$valuesClause = implode(',', $valuesClauseArr);

			$query = "UPDATE {$targetTable} a
			INNER JOIN {$sourceTable} b ON {$joinClause}
            SET {$valuesClause}
        ";

			$this->db->exec($query);

			// delete updated from temp table
			$query = "DELETE a FROM {$sourceTable} a
            INNER JOIN {$targetTable} b ON {$joinClause}
        ";
			$this->db->exec($query);
		}

		$columnsClause = implode(',', $columns);

		// insert new data
		$query = "INSERT INTO {$targetTable} ({$columnsClause}) SELECT * FROM {$sourceTable}";
		$this->db->exec($query);
	}
}
