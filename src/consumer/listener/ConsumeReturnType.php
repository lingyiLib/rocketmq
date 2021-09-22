<?php
namespace lingyiLib\rocketmq\consumer\listener;

class ConsumeReturnType
{
    const SUCCESS = "SUCCESS";

    const TIME_OUT = "TIME_OUT";

    const EXCEPTION = "EXCEPTION";

    const RETURNNULL = "RETURNNULL";

    const FAILED = "FAILED";
}