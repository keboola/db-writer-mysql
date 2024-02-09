<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Exception;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbWriterConfig\Exception\PropertyNotSetException;
use Keboola\Temp\Temp;
use PDO;
use PDOException;
use Psr\Log\LoggerInterface;

class MySQLConnectionFactory
{
    // Some SSL keys who worked in Debian Stretch (OpenSSL 1.1.0) stopped working in Debian Buster (OpenSSL 1.1.1).
    // Eg. "Signature Algorithm: sha1WithRSAEncryption" used in mysql5 tests in this repo.
    // This is because Debian wants to be "more secure"
    // and has set "SECLEVEL", which in OpenSSL defaults to "1", to value "2".
    // See https://wiki.debian.org/ContinuousIntegration/TriagingTips/openssl-1.1.1
    // So we reset this value to OpenSSL default.
    public const SSL_DEFAULT_CIPHER_CONFIG = 'DEFAULT@SECLEVEL=1';

    /**
     * @throws PropertyNotSetException|\Keboola\Component\UserException
     */
    public static function create(
        DatabaseConfig $databaseConfig,
        LoggerInterface $logger,
    ): MySQLConnection {
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, // convert errors to PDOExceptions
            PDO::MYSQL_ATTR_LOCAL_INFILE => true,
        ];

        // ssl encryption
        $isSsl = false;
        if ($databaseConfig->hasSslConfig()) {
            $sslConfig = $databaseConfig->getSslConfig();

            $temp = new Temp('wr-db-mysql');

            if ($sslConfig->hasKey()) {
                $options[PDO::MYSQL_ATTR_SSL_KEY] = self::createSSLFile($temp, $sslConfig->getKey());
                $isSsl = true;
            }
            if ($sslConfig->hasCert()) {
                $options[PDO::MYSQL_ATTR_SSL_CERT] = self::createSSLFile($temp, $sslConfig->getCert());
                $isSsl = true;
            }
            if ($sslConfig->hasCa()) {
                $options[PDO::MYSQL_ATTR_SSL_CA] = self::createSSLFile($temp, $sslConfig->getCa());
                $isSsl = true;
            }
            if ($sslConfig->hasCipher()) {
                $options[PDO::MYSQL_ATTR_SSL_CIPHER] = $sslConfig->getCipher();
            } else {
                $options[PDO::MYSQL_ATTR_SSL_CIPHER] = self::SSL_DEFAULT_CIPHER_CONFIG;
            }
            if (!$sslConfig->getVerifyServerCert()) {
                $options[PDO::MYSQL_ATTR_SSL_VERIFY_SERVER_CERT] = false;
            }
        }

        $dsn = sprintf(
            'mysql:host=%s;port=%s;dbname=%s',
            $databaseConfig->getHost(),
            $databaseConfig->hasPort() ? $databaseConfig->getPort() : '3306',
            $databaseConfig->getDatabase(),
        );

        $connection = new MySQLConnection(
            $logger,
            $dsn,
            $databaseConfig->getUser(),
            $databaseConfig->getPassword(),
            $options,
            function (PDO $connection) use ($isSsl, $logger): void {
                if ($isSsl) {
                    $statusStmt = $connection->query("SHOW STATUS LIKE 'Ssl_cipher';");
                    if (!$statusStmt) {
                        throw new UserException('Cannot get SSL status');
                    }
                    /** @var array $status */
                    $status = $statusStmt->fetch(PDO::FETCH_ASSOC);
                    if (empty($status['Value'])) {
                        throw new UserException(sprintf('Connection is not encrypted'));
                    } else {
                        $logger->info('Using SSL cipher: ' . $status['Value']);
                    }
                }

                $infileStmt = $connection->query("SHOW VARIABLES LIKE 'local_infile';");
                if (!$infileStmt) {
                    throw new UserException('Cannot get local_infile status');
                }

                /** @var array $infile */
                $infile = $infileStmt->fetch(PDO::FETCH_ASSOC);
                if ($infile['Value'] === 'OFF') {
                    throw new UserException('local_infile is disabled on server');
                }

                // for mysql8 remove sql_mode 'NO_ZERO_DATE'
                $versionStmt = $connection->query('SHOW VARIABLES LIKE "version";');
                if (!$versionStmt) {
                    throw new UserException('Cannot get MySQL version');
                }
                /** @var array $version */
                $version = $versionStmt->fetch(PDO::FETCH_ASSOC);
                if (version_compare($version['Value'], '8.0.0', '>')) {
                    $connection->query("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'NO_ZERO_DATE',''));");
                }
                $connection->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
            },
        );

        try {
            $connection->exec(sprintf('SET NAMES %s;', $connection->getCharset()));
        } catch (PDOException) {
            $logger->info('Falling back to ' . $connection->getCharset() . ' charset');
            $connection->setCharset('utf8');
            $connection->exec(sprintf('SET NAMES %s;', $connection->getCharset()));
        }

        return $connection;
    }

    /**
     * @throws Exception
     */
    private static function createSSLFile(Temp $temp, string $fileContent): string
    {
        $fileInfo = $temp->createTmpFile('ssl');
        file_put_contents($fileInfo->getPathname(), $fileContent);
        return $fileInfo->getPathname();
    }
}
