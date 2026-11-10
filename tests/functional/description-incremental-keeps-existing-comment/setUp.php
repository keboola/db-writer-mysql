<?php

declare(strict_types=1);

use Keboola\DbWriter\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $test->dumpComments = true;
    $test->connection->exec('CREATE TABLE `simple` (
        `id` INT NOT NULL PRIMARY KEY,
        `name` VARCHAR(255) COMMENT \'Existing name comment\',
        `glasses` VARCHAR(255) COMMENT \'Existing glasses comment\'
    ) COMMENT = \'Existing table comment\'');
    $test->connection->exec('INSERT INTO `simple` VALUES (0, \'Old name\', \'no\'), (100, \'Kept row\', \'yes\')');
};
