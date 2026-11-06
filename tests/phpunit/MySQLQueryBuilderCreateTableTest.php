<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Writer\MySQLQueryBuilder;
use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterAdapter\Query\DefaultQueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class MySQLQueryBuilderCreateTableTest extends TestCase
{
    /**
     * @dataProvider columnProvider
     * @param array{name: string, dbName: string, type: string, size?: string, nullable?: bool, default?: string} $item
     */
    public function testCreateTableColumnDefinition(array $item, string $expectedColumn): void
    {
        self::assertSame(
            sprintf('CREATE  TABLE `t` (%s) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;', $expectedColumn),
            (new MySQLQueryBuilder('utf8mb4'))->createQueryStatement(
                $this->createConnection(),
                't',
                false,
                [ItemConfig::fromArray($item)],
            ),
        );
    }

    public function columnProvider(): iterable
    {
        yield 'bit with default 0' => [
            self::item('bit', '0'),
            "`c` bit NOT NULL DEFAULT b'0'",
        ];
        yield 'bit with default 1' => [
            self::item('bit', '1'),
            "`c` bit NOT NULL DEFAULT b'1'",
        ];
        yield 'bit with multi-digit default' => [
            self::item('BIT', '101', '3'),
            "`c` BIT(3) NOT NULL DEFAULT b'101'",
        ];
        yield 'bit without default' => [
            self::item('bit'),
            '`c` bit NOT NULL ',
        ];
        yield 'bit with non-binary default keeps quoting' => [
            self::item('bit', '2'),
            "`c` bit NOT NULL DEFAULT '2'",
        ];
        yield 'varchar with default 0 stays quoted' => [
            self::item('varchar', '0', '255'),
            "`c` varchar(255) NOT NULL DEFAULT '0'",
        ];
    }

    public function testNonBitColumnsMatchDefaultQueryBuilder(): void
    {
        $items = [
            ItemConfig::fromArray(self::item('int', '0')),
            ItemConfig::fromArray(self::item('varchar', '1', '255', 'v', true)),
            ItemConfig::fromArray(self::item('TEXT', 'x', null, 'tx')),
            ItemConfig::fromArray(self::item('date', null, null, 'd', true)),
            ItemConfig::fromArray(self::item('ignore', '0', null, 'ign')),
        ];
        $connection = $this->createConnection();

        self::assertSame(
            (new DefaultQueryBuilder())->createQueryStatement($connection, 't', true, $items, ['c', 'v']),
            (new MySQLQueryBuilder('utf8'))->createQueryStatement($connection, 't', true, $items, ['c', 'v']),
        );
    }

    private function createConnection(): Connection&MockObject
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('quote')->willReturnCallback(fn(string $str) => "'" . addslashes($str) . "'");
        $connection->method('quoteIdentifier')->willReturnCallback(fn(string $str) => '`' . $str . '`');
        return $connection;
    }

    /**
     * @return array{name: string, dbName: string, type: string, size?: string, nullable: bool, default?: string}
     */
    private static function item(
        string $type,
        ?string $default = null,
        ?string $size = null,
        string $name = 'c',
        bool $nullable = false,
    ): array {
        $item = ['name' => $name, 'dbName' => $name, 'type' => $type, 'nullable' => $nullable];
        if ($size !== null) {
            $item['size'] = $size;
        }
        if ($default !== null) {
            $item['default'] = $default;
        }
        return $item;
    }
}
