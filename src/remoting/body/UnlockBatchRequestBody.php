<?php
namespace lingyiLib\rocketmq\remoting\body;

use lingyiLib\rocketmq\core\Column;
use lingyiLib\rocketmq\entity\MessageQueue;

class UnlockBatchRequestBody extends Column
{
    protected $consumerGroup;
    protected $clientId;
    protected $mqSet = [];

    /**
     * @return mixed
     */
    public function getConsumerGroup()
    {
        return $this->consumerGroup;
    }

    /**
     * @param mixed $consumerGroup
     */
    public function setConsumerGroup($consumerGroup)
    {
        $this->consumerGroup = $consumerGroup;
    }

    /**
     * @return mixed
     */
    public function getClientId()
    {
        return $this->clientId;
    }

    /**
     * @param mixed $clientId
     */
    public function setClientId($clientId)
    {
        $this->clientId = $clientId;
    }

    /**
     * @return array
     */
    public function getMqSet(): array
    {
        return $this->mqSet;
    }

    /**
     * @param array $mqSet
     */
    public function setMqSet(array $mqSet)
    {
        $this->mqSet = $mqSet;
    }

    /**
     * @param MessageQueue $me
     */
    public function addMq(MessageQueue $me)
    {
        $this->mqSet[] = $me;
    }
}