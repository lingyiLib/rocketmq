<?php
namespace lingyiLib\rocketmq\remoting\heartbeat;

use lingyiLib\rocketmq\core\Column;

class ProducerData extends Column
{
    protected $groupName;

    /**
     * @return mixed
     */
    public function getGroupName()
    {
        return $this->groupName;
    }

    /**
     * @param mixed $groupName
     */
    public function setGroupName($groupName)
    {
        $this->groupName = $groupName;
    }
}