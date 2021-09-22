<?php
namespace lingyiLib\rocketmq\consumer;

use Closure;
use lingyiLib\rocketmq\remoting\callback\InvokeCallback;
use lingyiLib\rocketmq\remoting\RemotingCommand;

class PullMessageCallback implements InvokeCallback
{
    /**
     * @var Closure
     */
    private $pullback;

    public function __construct(Closure $pullback){
        $this->pullback = $pullback;
    }

    function operationComplete(RemotingCommand $response){
        $pullback = $this->pullback;
        $pullback($response);
    }
}