<?php
namespace lingyiLib\rocketmq\remoting\processor;

use lingyiLib\rocketmq\MQAsyncClientInstance;
use lingyiLib\rocketmq\remoting\AbstractRemotingClient;
use lingyiLib\rocketmq\remoting\callback\InvokeCallback;
use lingyiLib\rocketmq\remoting\RemotingCommand;

class SubscriptionChangedProcessor implements Processor
{
    /**
     * @var MQAsyncClientInstance
     */
    private $mqClientFactory;

    private $doRebalanceComplated = false;

    /**
     * SubscriptionChangedProcessor constructor.
     * @param MQAsyncClientInstance $mqClientFactory
     */
    public function __construct(MQAsyncClientInstance $mqClientFactory)
    {
        $this->mqClientFactory = $mqClientFactory;
    }

    function execute(AbstractRemotingClient $client , RemotingCommand $remotingCommand , InvokeCallback $invokeCallback = null)
    {
        if(!$this->doRebalanceComplated){
            $this->mqClientFactory->doRebalance();
            $this->doRebalanceComplated = true;
        }
    }

    function exception(\Exception $e)
    {
        // TODO: Implement exception() method.
    }

}