<?php
namespace lingyiLib\rocketmq\remoting\callback;

use lingyiLib\rocketmq\remoting\AbstractRemotingClient;
use lingyiLib\rocketmq\remoting\RemotingCommand;

interface InvokeCallback
{
    function operationComplete(RemotingCommand $remotingCommand);
}