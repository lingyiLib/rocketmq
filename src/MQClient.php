<?php
namespace lingyiLib\rocketmq;

use lingyiLib\rocketmq\entity\TopicRouteData;
use lingyiLib\rocketmq\remoting\header\namesrv\GetRouteInfoRequestHeader;
use lingyiLib\rocketmq\remoting\NettyClient;
use lingyiLib\rocketmq\remoting\MessageDecoder;
use lingyiLib\rocketmq\remoting\MessageEncoder;
use lingyiLib\rocketmq\remoting\RemotingCommand;
use lingyiLib\rocketmq\remoting\RequestCode;
use lingyiLib\rocketmq\util\MQClientUtil;

class MQClient
{
    /**
     * @var NettyClient
     */
    private $nettyClient;

    public function __construct(){
        $this->nettyClient = new NettyClient();
    }


    /**
     * @param RemotingCommand $requestCommand
     * @return RemotingCommand
     * @throws exception\RocketMQClientException
     */
    public function sendMessage(RemotingCommand $requestCommand){
        $byteBuf = MessageEncoder::encode($requestCommand);
        $result = $this->nettyClient->send($byteBuf);
        $remotingCommand = MessageDecoder::decode($result);
        return $remotingCommand;
    }

    public function connect($addr){
        $this->nettyClient->connect($addr , 10);
    }

    public function shutdown(){
        $this->nettyClient->shutdown();
    }
}