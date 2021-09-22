<?php
namespace lingyiLib\rocketmq\consumer;

use lingyiLib\rocketmq\consumer\listener\ConsumeConcurrentlyContext;
use lingyiLib\rocketmq\entity\MessageExt;

interface MessageListenerConcurrently extends MessageListener
{
    /**
     * @param MessageExt[] $msgs
     * @param ConsumeConcurrentlyContext $context
     * @return string
     */
    function consumeMessage(array $msgs, ConsumeConcurrentlyContext $context);
}
