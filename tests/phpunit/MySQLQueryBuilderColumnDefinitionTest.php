<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Writer\MySQLQueryBuilder;
use Keboola\DbWriterAdapter\Connection\Connection;
use PHPUnit\Framework\TestCase;

class MySQLQueryBuilderColumnDefinitionTest extends TestCase
{
    /**
     * @dataProvider columnProvider
     * @param array{
     *     COLUMN_TYPE: string,
     *     IS_NULLABLE: string,
     *     COLUMN_DEFAULT: ?string,
     *     EXTRA: ?string,
     *     CHARACTER_SET_NAME: ?string,
     *     COLLATION_NAME: ?string,
     * } $column
     */
    public function testColumnDefinition(array $column, bool $isMariaDb, string $expected): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('quote')->willReturnCallback(fn(string $str) => "'" . addslashes($str) . "'");

        self::assertSame(
            $expected,
            (new MySQLQueryBuilder('utf8mb4'))->columnDefinition($connection, $column, $isMariaDb),
        );
    }

    public function columnProvider(): iterable
    {
        yield 'not null with literal default' => [
            self::column('int', 'NO', '0'),
            false,
            "int NOT NULL DEFAULT '0'",
        ];
        yield 'nullable without default' => [
            self::column('varchar(255)', 'YES', null, '', 'utf8mb4', 'utf8mb4_0900_ai_ci'),
            false,
            'varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL',
        ];
        yield 'current timestamp default with on update, MySQL 8' => [
            self::column('timestamp', 'NO', 'CURRENT_TIMESTAMP', 'DEFAULT_GENERATED on update CURRENT_TIMESTAMP'),
            false,
            'timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP',
        ];
        yield 'current timestamp default with on update, MySQL 5.7' => [
            self::column('timestamp(3)', 'NO', 'CURRENT_TIMESTAMP(3)', 'on update CURRENT_TIMESTAMP(3)'),
            false,
            'timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) on update CURRENT_TIMESTAMP(3)',
        ];
        yield 'varchar with collation' => [
            self::column('varchar(10)', 'NO', 'abc', '', 'utf8mb4', 'utf8mb4_bin'),
            false,
            "varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'abc'",
        ];
        yield 'quote in default value' => [
            self::column('varchar(20)', 'YES', 'it\'s "x"', '', 'utf8mb4', 'utf8mb4_bin'),
            false,
            'varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT \'it\\\'s \\"x\\"\'',
        ];
        yield 'literal NULL string default' => [
            self::column('varchar(4)', 'YES', 'NULL', '', 'utf8mb4', 'utf8mb4_bin'),
            false,
            "varchar(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT 'NULL'",
        ];
        yield 'expression default' => [
            self::column('varchar(36)', 'NO', 'uuid()', 'DEFAULT_GENERATED', 'utf8mb4', 'utf8mb4_bin'),
            false,
            'varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT (uuid())',
        ];
        yield 'expression default with escaped string literals' => [
            self::column(
                'varchar(50)',
                'NO',
                'concat(_utf8mb4\\\'x\\\\\\\\y\\\',_utf8mb4\\\'it\\\\\\\'s\\\',_utf8mb4\\\'q"\\\')',
                'DEFAULT_GENERATED',
                'utf8mb4',
                'utf8mb4_bin',
            ),
            false,
            'varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL '
            . 'DEFAULT (concat(_utf8mb4\'x\\\\y\',_utf8mb4\'it\\\'s\',_utf8mb4\'q"\'))',
        ];
        yield 'bit default' => [
            self::column('bit(1)', 'NO', "b'1'"),
            false,
            "bit(1) NOT NULL DEFAULT b'1'",
        ];
        yield 'auto increment' => [
            self::column('bigint unsigned', 'NO', null, 'auto_increment'),
            false,
            'bigint unsigned NOT NULL auto_increment',
        ];
        yield 'MariaDB literal default' => [
            self::column('varchar(10)', 'NO', "'it''s'", '', 'utf8mb4', 'utf8mb4_general_ci'),
            true,
            "varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'it''s'",
        ];
        yield 'MariaDB NULL default' => [
            self::column('int(11)', 'YES', 'NULL'),
            true,
            'int(11) NULL',
        ];
        yield 'MariaDB current timestamp with on update' => [
            self::column('timestamp', 'NO', 'current_timestamp()', 'on update current_timestamp()'),
            true,
            'timestamp NOT NULL DEFAULT current_timestamp() on update current_timestamp()',
        ];
    }

    /**
     * @return array{
     *     COLUMN_TYPE: string,
     *     IS_NULLABLE: string,
     *     COLUMN_DEFAULT: ?string,
     *     EXTRA: ?string,
     *     CHARACTER_SET_NAME: ?string,
     *     COLLATION_NAME: ?string,
     * }
     */
    private static function column(
        string $type,
        string $nullable,
        ?string $default,
        string $extra = '',
        ?string $charset = null,
        ?string $collation = null,
    ): array {
        return [
            'COLUMN_TYPE' => $type,
            'IS_NULLABLE' => $nullable,
            'COLUMN_DEFAULT' => $default,
            'EXTRA' => $extra,
            'CHARACTER_SET_NAME' => $charset,
            'COLLATION_NAME' => $collation,
        ];
    }
}
