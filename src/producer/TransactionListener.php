<?php
namespace lingyiLib\rocketmq\producer;

use lingyiLib\rocketmq\entity\Message;
use lingyiLib\rocketmq\entity\MessageExt;
use lingyiLib\rocketmq\entity\SendResult;

interface TransactionListener
{

    /**
     * 执行本地事务
     * @param Message $msg
     * @param SendResult $sendResult
     * @param $arg
     * @return mixed
     */
    function executeLocalTransaction(Message $msg , SendResult $sendResult, $arg);

    /**
     * 回查事务状态
     * @param MessageExt $msg
     * @return LocalTransactionState
     */
    function checkLocalTransaction(MessageExt $msg);
}