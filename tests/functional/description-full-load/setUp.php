<?php

declare(strict_types=1);

use Keboola\DbWriter\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $test->dumpComments = true;
};
