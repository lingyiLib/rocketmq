<?php
namespace lingyiLib\rocketmq\producer;

use lingyiLib\rocketmq\entity\Message;
use lingyiLib\rocketmq\entity\SendResult;

interface MQProducerFallback
{
    function beforeSendMessage(Message $message);

    function afterSendMessage(Message $message , SendResult $sendResult);
}