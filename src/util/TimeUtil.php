<?php
namespace lingyiLib\rocketmq\util;

class TimeUtil
{
    public static function currentTimeMillis(){
        return intval(microtime(true)*1000);
    }
}