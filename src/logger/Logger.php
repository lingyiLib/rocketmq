<?php
namespace lingyiLib\rocketmq\logger;

use Illuminate\Support\Facades\Log;

class Logger{
    const INFO = 'info';
    const DEBUG = 'debug';
    const WARN = 'warn';
    const ERROR = 'error';

    private $loggerHandler;

    public function __construct(){
        $this->loggerHandler = Log::channel('rocketmq');
    }

    private function formatMessage($message , ...$args){
        if(func_num_args() > 1) {
            return sprintf(str_replace("{}", "%s", $message), ...$args);
        }else{
            return $message;
        }
    }

    private function handle($level,$message,...$args){
        $message = $this->formatMessage($message,...$args);
        $this->loggerHandler->{$level}($message);
    }

    public function debug($message,...$args){
        $this->handle(self::DEBUG,$message  , ...$args);
    }

    public function info($message,...$args){
        $this->handle(self::INFO,$message,...$args);
    }

    public function warn($message , ...$args){
        $this->handle(self::WARN,$message,...$args);
    }

    public function error($message , ...$args){
        $this->handle(self::ERROR,$message,...$args);
    }
}