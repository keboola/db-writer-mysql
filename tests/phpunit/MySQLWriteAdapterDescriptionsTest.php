<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Writer\MySQLConnection;
use Keboola\DbWriter\Writer\MySQLQueryBuilder;
use Keboola\DbWriter\Writer\MySQLWriteAdapter;
use Keboola\DbWriterAdapter\ValueObject\TableDescriptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class MySQLWriteAdapterDescriptionsTest extends TestCase
{
    /** @var string[] */
    private array $executed = [];

    /** @var string[] */
    private array $fetched = [];

    /** @var string[] */
    private array $lockWaitTimeouts = [];

    /** @var array<int, array<string, ?string>> */
    private array $columns = [];

    private string $version = '8.4.0';

    private ?string $tableComment = '';

    private TestLogger $logger;

    private MySQLWriteAdapter $adapter;

    protected function setUp(): void
    {
        $this->executed = [];
        $this->fetched = [];
        $this->lockWaitTimeouts = [];
        $this->version = '8.4.0';
        $this->tableComment = '';
        $this->columns = [
            self::column('id', 'int', 'NO', null),
            self::column('name', 'varchar(255)', 'YES', null, 'utf8mb4', 'utf8mb4_0900_ai_ci'),
        ];
        $this->logger = new TestLogger();

        $connection = $this->createPartialMock(MySQLConnection::class, ['exec', 'fetchAll', 'quote']);
        $connection->method('quote')->willReturnCallback(fn(string $str) => "'" . addslashes($str) . "'");
        $connection->method('exec')->willReturnCallback(function (string $sql): void {
            if (str_starts_with($sql, 'SET SESSION lock_wait_timeout')) {
                $this->lockWaitTimeouts[] = $sql;
                return;
            }
            $this->executed[] = $sql;
        });
        $connection->method('fetchAll')->willReturnCallback(function (string $sql): array {
            if (str_contains($sql, 'lock_wait_timeout')) {
                return [['value' => '31536000']];
            }
            $this->fetched[] = $sql;
            if (str_starts_with($sql, 'SELECT VERSION()')) {
                return [['version' => $this->version]];
            }
            if (str_contains($sql, 'INFORMATION_SCHEMA.TABLES')) {
                return [['TABLE_COMMENT' => $this->tableComment]];
            }
            return $this->columns;
        });

        $this->adapter = new MySQLWriteAdapter($connection, new MySQLQueryBuilder('utf8mb4'), $this->logger);
    }

    public function testTableAndColumns(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions(
            'Table description',
            ['id' => 'Identifier', 'name' => 'Full name'],
        ));

        self::assertSame([
            'ALTER TABLE `simple` COMMENT = \'Table description\'',
            'ALTER TABLE `simple` MODIFY COLUMN `id` int NOT NULL COMMENT \'Identifier\', '
            . 'MODIFY COLUMN `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL '
            . 'COMMENT \'Full name\'',
        ], $this->executed);
        self::assertStringContainsString(
            'FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = \'simple\'',
            $this->fetched[1],
        );
    }

    public function testColumnsOnly(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions(null, ['name' => 'Full name']));

        self::assertSame([
            'ALTER TABLE `simple` MODIFY COLUMN `name` varchar(255) CHARACTER SET utf8mb4 '
            . 'COLLATE utf8mb4_0900_ai_ci NULL COMMENT \'Full name\'',
        ], $this->executed);
    }

    public function testTableOnlyDoesNotReadColumns(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions('Table description', []));

        self::assertSame(['ALTER TABLE `simple` COMMENT = \'Table description\''], $this->executed);
        self::assertCount(1, $this->fetched);
        self::assertStringContainsString('INFORMATION_SCHEMA.TABLES', $this->fetched[0]);
    }

    public function testMatchingCommentsIssueNoDdl(): void
    {
        $this->tableComment = 'Table description';
        $this->columns = [
            self::column('id', 'int', 'NO', null, null, null, '', 'Identifier'),
            self::column('name', 'varchar(255)', 'YES', null, 'utf8mb4', 'utf8mb4_0900_ai_ci', '', 'Full name'),
        ];

        $this->adapter->applyDescriptions('simple', new TableDescriptions(
            'Table description',
            ['id' => 'Identifier', 'name' => 'Full name'],
        ));

        self::assertSame([], $this->executed);
    }

    public function testOnlyChangedColumnIsModified(): void
    {
        $this->columns = [
            self::column('id', 'int', 'NO', null, null, null, '', 'Identifier'),
            self::column('name', 'varchar(255)', 'YES', null, 'utf8mb4', 'utf8mb4_0900_ai_ci', '', 'Old'),
        ];

        $this->adapter->applyDescriptions('simple', new TableDescriptions(
            null,
            ['id' => 'Identifier', 'name' => 'Full name'],
        ));

        self::assertSame([
            'ALTER TABLE `simple` MODIFY COLUMN `name` varchar(255) CHARACTER SET utf8mb4 '
            . 'COLLATE utf8mb4_0900_ai_ci NULL COMMENT \'Full name\'',
        ], $this->executed);
    }

    public function testLockWaitTimeoutIsLoweredAndRestored(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions(null, ['id' => 'Identifier']));

        self::assertSame([
            'SET SESSION lock_wait_timeout = 10',
            'SET SESSION lock_wait_timeout = 31536000',
        ], $this->lockWaitTimeouts);
    }

    public function testEmptyDescriptionsDoNothing(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions('', []));

        self::assertSame([], $this->executed);
        self::assertSame([], $this->fetched);
    }

    public function testQuoting(): void
    {
        $this->columns = [self::column('col `x`', 'int', 'NO', null)];

        $this->adapter->applyDescriptions('my `table`', new TableDescriptions(
            'It\'s a "table" with \\ backslash',
            ['col `x`' => 'Bob\'s column'],
        ));

        self::assertSame([
            'ALTER TABLE `my ``table``` COMMENT = \'It\\\'s a \\"table\\" with \\\\ backslash\'',
            'ALTER TABLE `my ``table``` MODIFY COLUMN `col ``x``` int NOT NULL COMMENT \'Bob\\\'s column\'',
        ], $this->executed);
        self::assertStringContainsString('TABLE_NAME = \'my `table`\'', $this->fetched[0]);
    }

    public function testColumnNameMatchIsCaseInsensitive(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions(null, ['ID' => 'Identifier']));

        self::assertSame([
            'ALTER TABLE `simple` MODIFY COLUMN `id` int NOT NULL COMMENT \'Identifier\'',
        ], $this->executed);
    }

    public function testNumericColumnName(): void
    {
        $this->columns = [self::column('123', 'int', 'YES', null)];
        // PHP casts numeric string keys to int at runtime.
        /** @var array<string, string> $columns */
        $columns = ['123' => 'Numeric name'];

        $this->adapter->applyDescriptions('simple', new TableDescriptions(null, $columns));

        self::assertSame([
            'ALTER TABLE `simple` MODIFY COLUMN `123` int NULL COMMENT \'Numeric name\'',
        ], $this->executed);
    }

    public function testMissingColumnIsSkippedWithWarning(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions(
            null,
            ['missing' => 'Nope', 'id' => 'Identifier'],
        ));

        self::assertSame([
            'ALTER TABLE `simple` MODIFY COLUMN `id` int NOT NULL COMMENT \'Identifier\'',
        ], $this->executed);
        self::assertTrue($this->logger->hasWarning(
            'Column "missing" was not found in table "simple", its description was not written.',
        ));
    }

    public function testOnlyMissingColumnsExecuteNothing(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions(null, ['missing' => 'Nope']));

        self::assertSame([], $this->executed);
    }

    public function testGeneratedColumnIsSkippedWithWarning(): void
    {
        $this->columns = [self::column('total', 'int', 'YES', null, null, null, 'STORED GENERATED')];

        $this->adapter->applyDescriptions('simple', new TableDescriptions(null, ['total' => 'Sum']));

        self::assertSame([], $this->executed);
        self::assertTrue($this->logger->hasWarning(
            'Column "total" in table "simple" is a generated column, its description was not written.',
        ));
    }

    public function testSpatialColumnIsSkippedWithWarning(): void
    {
        $this->columns = [self::column('area', 'point', 'NO', null)];

        $this->adapter->applyDescriptions('simple', new TableDescriptions(null, ['area' => 'Location']));

        self::assertSame([], $this->executed);
        self::assertTrue($this->logger->hasWarning(
            'Column "area" in table "simple" is a spatial column, its description was not written.',
        ));
    }

    public function testTruncation(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions(
            str_repeat('č', 2050),
            ['id' => str_repeat('ž', 1030)],
        ));

        self::assertSame([
            sprintf('ALTER TABLE `simple` COMMENT = \'%s\'', str_repeat('č', 2048)),
            sprintf(
                'ALTER TABLE `simple` MODIFY COLUMN `id` int NOT NULL COMMENT \'%s\'',
                str_repeat('ž', 1024),
            ),
        ], $this->executed);
        self::assertTrue($this->logger->hasWarning(
            'Description of table "simple" is longer than 2048 characters, it was truncated.',
        ));
        self::assertTrue($this->logger->hasWarning(
            'Description of column "id" in table "simple" is longer than 1024 characters, it was truncated.',
        ));
    }

    public function testMaxLengthIsNotTruncated(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions(str_repeat('a', 2048), []));

        self::assertSame(
            [sprintf('ALTER TABLE `simple` COMMENT = \'%s\'', str_repeat('a', 2048))],
            $this->executed,
        );
        self::assertFalse($this->logger->hasWarningRecords());
    }

    public function testMariaDbDefaultIsKeptAsExpression(): void
    {
        $this->version = '10.11.6-MariaDB-1:10.11.6+maria~ubu2204';
        $this->columns = [self::column('name', 'varchar(10)', 'NO', "'abc'", 'utf8mb4', 'utf8mb4_general_ci')];

        $this->adapter->applyDescriptions('simple', new TableDescriptions(null, ['name' => 'Name']));

        self::assertSame([
            'ALTER TABLE `simple` MODIFY COLUMN `name` varchar(10) CHARACTER SET utf8mb4 '
            . 'COLLATE utf8mb4_general_ci NOT NULL DEFAULT \'abc\' COMMENT \'Name\'',
        ], $this->executed);
    }

    /**
     * @return array<string, ?string>
     */
    private static function column(
        string $name,
        string $type,
        string $nullable,
        ?string $default,
        ?string $charset = null,
        ?string $collation = null,
        string $extra = '',
        string $comment = '',
    ): array {
        return [
            'COLUMN_NAME' => $name,
            'COLUMN_TYPE' => $type,
            'IS_NULLABLE' => $nullable,
            'COLUMN_DEFAULT' => $default,
            'EXTRA' => $extra,
            'CHARACTER_SET_NAME' => $charset,
            'COLLATION_NAME' => $collation,
            'COLUMN_COMMENT' => $comment,
        ];
    }
}
