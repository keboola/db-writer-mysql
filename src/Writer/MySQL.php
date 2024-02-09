<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\Component\UserException;
use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterAdapter\WriteAdapter;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbWriterConfig\Exception\PropertyNotSetException;

class MySQL extends BaseWriter
{

    /** @var MySQLConnection $connection */
    protected Connection $connection;

    /**
     * @throws UserException|PropertyNotSetException
     */
    protected function createConnection(DatabaseConfig $databaseConfig): Connection
    {
        return MySQLConnectionFactory::create($databaseConfig, $this->logger);
    }

    protected function createWriteAdapter(): WriteAdapter
    {
        return new MySQLWriteAdapter(
            $this->connection,
            new MySQLQueryBuilder($this->connection->getCharset()),
            $this->logger,
        );
    }
}
