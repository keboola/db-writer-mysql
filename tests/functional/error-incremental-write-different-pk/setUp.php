<?php

declare(strict_types=1);

use Keboola\DbWriter\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    // create table with all column types
    $test->connection->exec('CREATE TABLE `incremental` (
        `name` VARCHAR(255),
        `int` int PRIMARY KEY,
        `float` FLOAT,
        `date` DATE,
        `datetime` DATETIME,
        `timestamp` TIMESTAMP
    )');
};
