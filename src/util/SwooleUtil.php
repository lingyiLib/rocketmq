<?php
namespace lingyiLib\rocketmq\util;

class SwooleUtil
{
    public static function isSwoole(){
        return defined("ENV_RUN") && ENV_RUN == "swoole";
    }
}