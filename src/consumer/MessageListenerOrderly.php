<?php
namespace lingyiLib\rocketmq\consumer;

use lingyiLib\rocketmq\consumer\listener\ConsumeOrderlyContext;
use lingyiLib\rocketmq\consumer\listener\ConsumeOrderlyStatus;
use lingyiLib\rocketmq\entity\MessageExt;

interface MessageListenerOrderly extends MessageListener
{
    /**
     * @param MessageExt[] $msgs
     * @param ConsumeOrderlyContext $context
     * @return ConsumeOrderlyStatus
     */
    function consumeMessage(array $msgs, ConsumeOrderlyContext $context);
}