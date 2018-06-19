<?php

declare(strict_types=1);

use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\MySQLApplication;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Handler\NullHandler;
use Keboola\DbWriter\Configuration\MySQLConfigDefinition;

require_once(dirname(__FILE__) . "/vendor/autoload.php");

$criticalHandler = new StreamHandler('php://stderr');
$criticalHandler->setBubble(false);
$criticalHandler->setLevel(Logger::CRITICAL);
$criticalHandler->setFormatter(new LineFormatter("[%datetime%] %level_name%: %message% %context% %extra%\n"));
$errorHandler = new StreamHandler('php://stderr');
$errorHandler->setBubble(false);
$errorHandler->setLevel(Logger::WARNING);
$errorHandler->setFormatter(new LineFormatter("%message%\n"));
$logHandler = new StreamHandler('php://stdout');
$logHandler->setBubble(false);
$logHandler->setLevel(Logger::INFO);
$logHandler->setFormatter(new LineFormatter("%message%\n"));
$logger = new Logger('wr-db-mysql');
$logger->setHandlers([$criticalHandler, $errorHandler, $logHandler]);

$action = 'run';

try {
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }
    $config = json_decode(file_get_contents($arguments["data"] . "/config.json"), true);
    $config['parameters']['data_dir'] = $arguments['data'];
    $config['parameters']['writer_class'] = 'MySQL';

    $action = isset($config['action']) ? $config['action'] : $action;

    $app = new MySQLApplication($config, $logger, new MySQLConfigDefinition());

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
    }
    echo $app->run();
} catch (UserException $e) {
    $logger->error($e->getMessage());

    if ($action !== 'run') {
        echo $e->getMessage();
    }

    exit(1);
} catch (ApplicationException $e) {
    $logger->critical(
        get_class($e) . ':' . $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'errCode' => $e->getCode(),
            'errTrace' => $e->getTraceAsString(),
            'errPrevious' => $e->getPrevious() ? get_class($e->getPrevious()) : '',
        ]
    );
    exit($e->getCode() > 1 ? $e->getCode(): 2);
} catch (\Throwable $e) {
    $logger->critical(
        get_class($e) . ':' . $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'errCode' => $e->getCode(),
            'errTrace' => $e->getTraceAsString(),
            'errPrevious' => $e->getPrevious() ? get_class($e->getPrevious()) : '',
        ]
    );
    exit(2);
}
exit(0);
