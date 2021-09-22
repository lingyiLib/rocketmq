<?php
namespace lingyiLib\rocketmq\remoting\processor;

use lingyiLib\rocketmq\remoting\AbstractRemotingClient;
use lingyiLib\rocketmq\remoting\callback\InvokeCallback;
use lingyiLib\rocketmq\remoting\RemotingCommand;

interface Processor
{
    function execute(AbstractRemotingClient $client , RemotingCommand $remotingCommand);

    function exception(\Exception $e);
}