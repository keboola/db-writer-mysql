<?php

use Keboola\DbWriter\Application;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Symfony\Component\Yaml\Yaml;
use Monolog\Handler\NullHandler;
use Keboola\DbWriter\Configuration\MySQLConfigDefinition;

define('APP_NAME', 'wr-db-mysql');
define('ROOT_PATH', __DIR__);

require_once(dirname(__FILE__) . "/vendor/keboola/db-writer-common/bootstrap.php");

$logger = new Logger(APP_NAME);

$action = 'run';

try {
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }
    $config = Yaml::parse(file_get_contents($arguments["data"] . "/config.yml"));
    $config['parameters']['data_dir'] = $arguments['data'];
    $config['parameters']['writer_class'] = 'MySQL';

    $action = isset($config['action']) ? $config['action'] : $action;

    var_dump($action);

    if ($action !== 'run') {
        $logger->setHandlers(array(new NullHandler(Logger::INFO)));
    }

    $app = new Application($config);
    $app->setConfigDefinition(new MySQLConfigDefinition());
    echo json_encode($app->run());
} catch(UserException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());

    if ($action !== 'run') {
        echo $e->getMessage();
    }

    exit(1);
} catch(ApplicationException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());
    exit($e->getCode() > 1 ? $e->getCode(): 2);
} catch(\Exception $e) {
    $logger->log('error', $e->getMessage(), [
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
        'trace' => $e->getTrace()
    ]);
    exit(2);
}
exit(0);
