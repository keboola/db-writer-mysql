<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use ErrorException;
use Iterator;
use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;
use Keboola\SSHTunnel\SSH;
use Keboola\SSHTunnel\SSHException;
use Keboola\Temp\Temp;
use PDO;
use PDOException;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use Throwable;

class MySQL extends Writer implements WriterInterface
{
    public const DEFAULT_MAX_TRIES = 5;

    /** @var array $variableColumns */
    private $variableColumns = [];

    /** @var array */
    private static $allowedTypes = [
        'int', 'smallint', 'bigint',
        'decimal', 'float', 'double',
        'date', 'datetime', 'timestamp',
        'char', 'varchar', 'text', 'blob',
    ];

    /** @var PDO */
    protected $db;

    /** @var string */
    private $charset = 'utf8mb4';

    public function generateTmpName(string $tableName): string
    {
        $tmpId = '_temp_' . uniqid();
        return mb_substr($tableName, 0, 30 - mb_strlen($tmpId)) . $tmpId;
    }

    public function createConnection(array $dbParams): PDO
    {
        $isSsl = false;

        // convert errors to PDOExceptions
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::MYSQL_ATTR_LOCAL_INFILE => true,
        ];

        if (!isset($dbParams['password']) && isset($dbParams['#password'])) {
            $dbParams['password'] = $dbParams['#password'];
        }

        // check params
        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf('Parameter %s is missing.', $r));
            }
        }

        // ssl encryption
        if (!empty($dbParams['ssl']) && !empty($dbParams['ssl']['enabled'])) {
            $ssl = $dbParams['ssl'];

            $temp = new Temp('wr-db-mysql');

            if (!empty($ssl['#key'])) {
                $options[PDO::MYSQL_ATTR_SSL_KEY] = $this->createSSLFile($ssl['#key'], $temp);
                $isSsl = true;
            }
            if (!empty($ssl['cert'])) {
                $options[PDO::MYSQL_ATTR_SSL_CERT] = $this->createSSLFile($ssl['cert'], $temp);
                $isSsl = true;
            }
            if (!empty($ssl['ca'])) {
                $options[PDO::MYSQL_ATTR_SSL_CA] = $this->createSSLFile($ssl['ca'], $temp);
                $isSsl = true;
            }
            if (!empty($ssl['cipher'])) {
                $options[PDO::MYSQL_ATTR_SSL_CIPHER] = $ssl['cipher'];
            }
            if (!$ssl['verifyServerCert']) {
                $options[PDO::MYSQL_ATTR_SSL_VERIFY_SERVER_CERT] = false;
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '3306';

        $dsn = sprintf(
            'mysql:host=%s;port=%s;dbname=%s',
            $dbParams['host'],
            $port,
            $dbParams['database']
        );
        $this->logger->info("Connecting to DSN '" . $dsn . "' " . ($isSsl ? 'Using SSL' : ''));

        try {
            $pdo = new PDO($dsn, $dbParams['user'], $dbParams['password'], $options);
        } catch (PDOException $e) {
            $this->handleException($e);
        }
        $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        try {
            $pdo->exec("SET NAMES $this->charset;");
        } catch (PDOException $e) {
            $this->charset = 'utf8';
            $this->logger->info('Falling back to ' . $this->charset . ' charset');
            $pdo->exec("SET NAMES $this->charset;");
        }
        $infile = $pdo->query("SHOW VARIABLES LIKE 'local_infile';")->fetch(PDO::FETCH_ASSOC);
        if ($infile['Value'] === 'OFF') {
            throw new UserException('local_infile is disabled on server');
        }

        if ($isSsl) {
            $status = $pdo->query("SHOW STATUS LIKE 'Ssl_cipher';")->fetch(PDO::FETCH_ASSOC);

            if (empty($status['Value'])) {
                throw new UserException(sprintf('Connection is not encrypted'));
            } else {
                $this->logger->info('Using SSL cipher: ' . $status['Value']);
            }
        }

        // for mysql8 remove sql_mode 'NO_ZERO_DATE'
        if (version_compare($this->getVersion($pdo), '8.0.0', '>')) {
            $pdo->query("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'NO_ZERO_DATE',''));");
        }

        return $pdo;
    }

    protected function exec(string $query): void
    {
        $this->logger->debug(sprintf('Executing query: "%s"', $query));

        try {
            $this->db->exec($query);
            $this->logMysqlWarnings();
        } catch (PDOException|ErrorException $e) {
            throw new UserException('Query failed: ' . $e->getMessage(), 400, $e, [
                'query' => $query,
            ]);
        }
    }

    public function write(CsvFile $csv, array $table): void
    {
        $header = $csv->getHeader();
        $columnNames = $this->columnNamesForLoad($table, $header);
        $csv->rewind();

        $query = "
            LOAD DATA LOCAL INFILE '{$csv}'
            INTO TABLE {$this->escape($table['dbName'])}
            CHARACTER SET $this->charset
            FIELDS TERMINATED BY ','            
            OPTIONALLY ENCLOSED BY '\"'
            ESCAPED BY ''
            IGNORE 1 LINES
            (". implode(', ', $columnNames) . ')
            ' . $this->getExpressionReplace($table)
        ;

        $this->logger->info(sprintf('Loading data to table "%s"', $table['dbName']));
        $this->exec($query);
    }

    protected function getExpressionReplace(array $table): string
    {
        $expressionsNullOrDefault = $this->emptyToNullOrDefault($table);
        $expressionsReplaceValues = $this->replaceColumnsValues($table);

        $expressions = array_merge(
            iterator_to_array($expressionsNullOrDefault),
            iterator_to_array($expressionsReplaceValues)
        );

        if (!$expressions) {
            return '';
        }

        return 'SET ' . implode(',', ($expressions));
    }

    protected function emptyToNullOrDefault(array $table): Iterator
    {
        $columnsWithNullOrDefault = array_filter($table['items'], function ($column) {
            return $this->hasColumnNullOrDefault($column);
        });

        foreach ($columnsWithNullOrDefault as $column) {
            switch (strtolower($column['type'])) {
                case 'date':
                case 'datetime':
                    $defaultValue = !empty($column['default']) ? $this->db->quote($column['default']) : 'NULL';
                    break;
                default:
                    if (($column['default'] ?? '') !== '') {
                        $defaultValue = $this->db->quote($column['default']);
                    } elseif (!empty($column['nullable'])) {
                        $defaultValue = 'NULL';
                    } else {
                        $defaultValue = $this->db->quote('');
                    }
            }

            yield sprintf(
                "%s = IF(%s = '', %s, %s)",
                $this->escape($column['dbName']),
                $this->getVariableColumn($column['dbName']),
                $defaultValue,
                $this->getVariableColumn($column['dbName'])
            );
        }
    }

    protected function replaceColumnsValues(array $table): Iterator
    {
        foreach ($table['items'] as $column) {
            switch (strtolower($column['type'])) {
                case 'bit':
                    if ($column['nullable']) {
                        yield sprintf(
                            "%s = IF(%s = '', NULL, cast(%s as signed))",
                            $this->escape($column['dbName']),
                            $this->getVariableColumn($column['dbName']),
                            $this->getVariableColumn($column['dbName'])
                        );
                    } else {
                        yield sprintf(
                            '%s = cast(%s as signed)',
                            $this->escape($column['dbName']),
                            $this->getVariableColumn($column['dbName'])
                        );
                    }
            }
        }
    }

    protected function columnNamesForLoad(array $table, array $header): array
    {
        return array_map(function ($column) use ($table) {
            // skip ignored
            foreach ($table['items'] as $tableColumn) {
                if ($tableColumn['name'] === $column && $tableColumn['type'] === 'IGNORE') {
                    return '@dummy';
                }
            }

            // name by mapping
            foreach ($table['items'] as $key => $tableColumn) {
                if ($tableColumn['name'] === $column) {
                    if ($this->hasColumnNullOrDefault($tableColumn) ||
                        $this->hasColumnForReplaceValues($tableColumn)
                    ) {
                        return $this->generateColumnVariable($tableColumn['dbName'], $key);
                    }
                    return $this->escape($tableColumn['dbName']);
                }
            }

            // origin sapi name
            return $this->escape($column);
        }, $header);
    }

    private function generateColumnVariable(string $columnName, int $columnKey): string
    {
        $variable = '@columnVar_' . $columnKey;
        $this->variableColumns[$columnName] = $variable;
        return $variable;
    }

    private function getVariableColumn(string $columnName): string
    {
        if (!isset($this->variableColumns[$columnName])) {
            throw new ApplicationException(sprintf('Variable for column "%s" does not exists.', $columnName));
        }
        return $this->variableColumns[$columnName];
    }

    private function hasColumnNullOrDefault(array $column): bool
    {
        $hasDefault = isset($column['default']);
        $isNullable = $column['nullable'] ?? false;
        $isIgnored = strtolower($column['type']) === 'ignore';
        return !$isIgnored && ($hasDefault || $isNullable);
    }

    private function hasColumnForReplaceValues(array $column): bool
    {
        return in_array(strtolower($column['type']), ['bit']);
    }

    public function drop(string $tableName): void
    {
        $this->exec(sprintf('DROP TABLE IF EXISTS %s;', $this->escape($tableName)));
    }

    private function escape(string $obj): string
    {
        return "`{$obj}`";
    }

    public function create(array $table): void
    {
        $sql = sprintf(
            'CREATE %s TABLE `%s` (',
            isset($table['temporary']) && $table['temporary'] === true ? 'TEMPORARY' : '',
            $table['dbName']
        );

        $columns = $table['items'];
        foreach ($columns as $k => $col) {
            $type = strtoupper($col['type']);
            if ($type === 'IGNORE') {
                continue;
            }

            if (!empty($col['size'])) {
                $type .= "({$col['size']})";
            }

            $null = $col['nullable'] ? 'NULL' : 'NOT NULL';

            $default = empty($col['default']) ? '' : "DEFAULT '" . $col['default'] . "'";
            if ($type === 'TEXT') {
                $default = '';
            }

            $sql .= $this->escape($col['dbName']) . " $type $null $default";
            $sql .= ',';
        }

        if (!empty($table['primaryKey'])) {
            $writer = $this;
            $sql .= 'PRIMARY KEY (' . implode(
                ', ',
                array_map(
                    function ($primaryColumn) use ($writer) {
                        return $writer->escape($primaryColumn);
                    },
                    $table['primaryKey']
                )
            ) . ')';

            $sql .= ',';
        }

        $sql = substr($sql, 0, -1);
        $sql .= ") DEFAULT CHARSET=$this->charset COLLATE {$this->charset}_unicode_ci";

        $this->exec($sql);
    }

    public static function getAllowedTypes(): array
    {
        return self::$allowedTypes;
    }

    public function upsert(array $table, string $targetTable): void
    {
        $this->logger->info(sprintf('Upserting table "%s" via "%s"', $targetTable, $table['dbName']));

        $columns = array_filter($table['items'], function ($item) {
            return $item['type'] !== 'IGNORE';
        });

        $dbColumns = array_map(function ($item) {
            return $this->escape($item['dbName']);
        }, $columns);

        if (!empty($table['primaryKey'])) {
            $this->upsertWithPK($table, $targetTable, $dbColumns);
            return;
        }

        $this->upsertWithoutPK($table, $targetTable, $dbColumns);
    }

    private function upsertWithPK(array $tableConfig, string $targetTable, array $dbColumns): void
    {
        // check primary keys
        $this->checkKeys($tableConfig['primaryKey'], $targetTable);

        // update data
        $tempTableName = $this->escape($tableConfig['dbName']);
        $targetTableName = $this->escape($targetTable);

        $valuesClauseArr = [];
        foreach ($dbColumns as $index => $column) {
            $valuesClauseArr[] = "{$targetTableName}.{$column}={$tempTableName}.{$column}";
        }
        $valuesClause = implode(',', $valuesClauseArr);
        $columnsClause = implode(',', $dbColumns);

        $query = "
          INSERT INTO {$targetTableName} ({$columnsClause})
          SELECT * FROM {$tempTableName}
          ON DUPLICATE KEY UPDATE
          {$valuesClause}
        ";

        $this->exec($query);

        $this->logger->info('Table "' . $tableConfig['dbName'] . '" upserted.');
    }

    private function upsertWithoutPK(array $tableConfig, string $targetTable, array $dbColumns): void
    {
        $columnsClause = implode(',', $dbColumns);

        // insert new data
        $this->exec("
          INSERT INTO {$this->escape($targetTable)} ({$columnsClause})
          SELECT * FROM {$this->escape($tableConfig['dbName'])}
        ");

        $this->logger->info('Table "' . $tableConfig['dbName'] . '" upserted.');
    }

    private function getKeysFromDbTable(string $tableName, string $keyName = 'PRIMARY'): array
    {
        $stmt = $this->db->query("SHOW KEYS FROM {$this->escape($tableName)} WHERE Key_name = '{$keyName}'");
        $result = $stmt->fetchAll();

        return array_map(function ($item) {
            return $item['Column_name'];
        }, $result);
    }

    public function checkKeys(array $configKeys, string $targetTable): void
    {
        $primaryKeysInDb = $this->getKeysFromDbTable($targetTable);
        sort($primaryKeysInDb);
        sort($configKeys);

        if ($primaryKeysInDb !== $configKeys) {
            throw new UserException(sprintf(
                'Primary key(s) in configuration does NOT match with keys in DB table.' . PHP_EOL
                . 'Keys in configuration: %s' . PHP_EOL
                . 'Keys in DB table: %s',
                implode(',', $configKeys),
                implode(',', $primaryKeysInDb)
            ));
        }
    }

    private function createSSLFile(string $sslCa, Temp $temp): string
    {
        $fileInfo = $temp->createTmpFile('ssl');
        file_put_contents($fileInfo->getPathname(), $sslCa);
        return $fileInfo->getPathname();
    }

    public function testConnection(): void
    {
        $this->db->query('SELECT NOW();')->execute();
    }

    public function tableExists(string $tableName): bool
    {
        $stmt = $this->db->prepare('SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?;');
        $stmt->execute([$tableName]);

        $res = $stmt->fetchAll();
        return !empty($res);
    }

    public function showTables(string $dbName): array
    {
        $stmt = $this->db->query('SHOW TABLES;');
        $res = $stmt->fetchAll(PDO::FETCH_ASSOC);

        return array_map(function ($item) {
            return array_shift($item);
        }, $res);
    }

    public function getTableInfo(string $tableName): array
    {
        $stmt = $this->db->query(sprintf('DESCRIBE %s;', $this->escape($tableName)));
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    public function validateTable(array $tableConfig): void
    {
        // TODO: Implement validateTable() method.
    }

    private function getVersion(PDO $pdo): string
    {
        $stmt = $pdo->query('SHOW VARIABLES LIKE "version";')->fetch(PDO::FETCH_ASSOC);
        return $stmt['Value'];
    }

    public function createSshTunnel(array $dbConfig): array
    {
        $sshConfig = $dbConfig['ssh'];

        // check params
        foreach (['keys', 'sshHost'] as $k) {
            if (empty($sshConfig[$k])) {
                throw new UserException(sprintf('Parameter %s is missing.', $k));
            }
        }

        if (empty($sshConfig['user'])) {
            $sshConfig['user'] = $dbConfig['user'];
        }
        if (empty($sshConfig['localPort'])) {
            $sshConfig['localPort'] = 33006;
        }
        if (empty($sshConfig['remoteHost'])) {
            $sshConfig['remoteHost'] = $dbConfig['host'];
        }
        if (empty($sshConfig['remotePort'])) {
            $sshConfig['remotePort'] = $dbConfig['port'];
        }
        if (empty($sshConfig['sshPort'])) {
            $sshConfig['sshPort'] = 22;
        }

        $sshConfig['privateKey'] = empty($sshConfig['keys']['#private'])
            ?$sshConfig['keys']['private']
            :$sshConfig['keys']['#private'];

        $tunnelParams = array_intersect_key($sshConfig, array_flip([
            'user', 'sshHost', 'sshPort', 'localPort', 'remoteHost', 'remotePort', 'privateKey',
        ]));

        $this->logger->info("Creating SSH tunnel to '" . $tunnelParams['sshHost'] . "'");

        $simplyRetryPolicy = new SimpleRetryPolicy(
            self::DEFAULT_MAX_TRIES,
            [SSHException::class, Throwable::class]
        );

        $exponentialBackOffPolicy = new ExponentialBackOffPolicy();
        $proxy = new RetryProxy(
            $simplyRetryPolicy,
            $exponentialBackOffPolicy,
            $this->logger
        );

        try {
            $proxy->call(function () use ($tunnelParams): void {
                $ssh = new SSH();
                $ssh->openTunnel($tunnelParams);
            });
        } catch (SSHException $e) {
            throw new UserException($e->getMessage());
        }

        $dbConfig['host'] = '127.0.0.1';
        $dbConfig['port'] = $sshConfig['localPort'];

        return $dbConfig;
    }

    protected function logMysqlWarnings(): void
    {
        $stmt = $this->db->query('SHOW WARNINGS;');
        $warnings = $stmt->fetchAll();
        foreach ($warnings as $warning) {
            $this->logger->warning($warning['Message']);
        }
    }

    private function handleException(Throwable $e): void
    {
        $checkCnMismatch = function (Throwable $exception): void {
            if (strpos($exception->getMessage(), 'did not match expected CN') !== false) {
                throw new UserException($exception->getMessage());
            }
        };
        $checkCnMismatch($e);
        $previous = $e->getPrevious();
        if ($previous !== null) {
            $checkCnMismatch($previous);
        }

        // SQLSTATE[HY000] is a main general message
        // additional informations are in the previous exception, so throw previous
        if (strpos($e->getMessage(), 'SQLSTATE[HY000]') === 0 && $e->getPrevious() !== null) {
            throw $e->getPrevious();
        }
        throw $e;
    }
}
