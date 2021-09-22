<?php
namespace lingyiLib\rocketmq\util;

class StringUtil
{
    /**
     * @param $str
     * @return bool
     */
    public static function toBool($str){
        if(strtoupper($str) == "FALSE"){
            return false;
        }
        return boolval($str);
    }
}