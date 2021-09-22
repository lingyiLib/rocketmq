<?php
namespace lingyiLib\rocketmq\remoting\processor;

use Illuminate\Support\Facades\Log;
use lingyiLib\rocketmq\cache\Cache;
use lingyiLib\rocketmq\entity\MessageConst;
use lingyiLib\rocketmq\entity\MessageSysFlag;
use lingyiLib\rocketmq\remoting\AbstractRemotingClient;
use lingyiLib\rocketmq\remoting\ByteBuf;
use lingyiLib\rocketmq\remoting\callback\InvokeCallback;
use lingyiLib\rocketmq\remoting\header\broker\EndTransactionRequestHeader;
use lingyiLib\rocketmq\remoting\MessageDecoder;
use lingyiLib\rocketmq\remoting\RemotingCommand;
use lingyiLib\rocketmq\remoting\RequestCode;
use lingyiLib\rocketmq\producer\LocalTransactionState;
use lingyiLib\rocketmq\RocketMQConfig;

class EndTransactionProcessor implements Processor
{
    /**
     * @var Cache
     */
    private $cache;
    /**
     * @var Log
     */
    private $log;
    
    public function __construct()
    {
        $this->cache = RocketMQConfig::getCache();
        $this->log = Log::channel('rocketmq');
    }

    /**
     * 事务回查
     * @param AbstractRemotingClient $client
     * @param RemotingCommand $remotingCommand
     * @param InvokeCallback|null $invokeCallback
     */
    public function execute(AbstractRemotingClient $client , RemotingCommand $remotingCommand , InvokeCallback $invokeCallback = null){
        $body = $remotingCommand->getBody();
        $byteBuf = new ByteBuf($body);
        $messageExt = MessageDecoder::decodeMessage($byteBuf , $remotingCommand);
        $producerGroup = $messageExt->getProperty(MessageConst::PROPERTY_PRODUCER_GROUP);
        $topic = $messageExt->getTopic();
        $transactionListener = $this->cache->getTransactionListener($producerGroup , $topic);
        if(!empty($transactionListener)){
            $remark = null;
            try{
                $localTransactionState = $transactionListener->checkLocalTransaction($messageExt);
            }catch (\Exception $e){
                $localTransactionState = LocalTransactionState::UNKNOW;
                $remark = $e->getMessage();
                $this->log->error("check transaction statr error");
            }
            // 发送响应
            $endTransactionRequestHeader = new EndTransactionRequestHeader();
            $endTransactionRequestHeader->setMsgId($messageExt->getMsgId());
            $endTransactionRequestHeader->setCommitLogOffset($messageExt->getCommitLogOffset());
            $endTransactionRequestHeader->setTranStateTableOffset($messageExt->getTranStateTableOffset());
            $endTransactionRequestHeader->setProducerGroup($producerGroup);
            $endTransactionRequestHeader->setFromTransactionCheck(true);
            $uniqueKey = $messageExt->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
            if ($uniqueKey == null) {
                $uniqueKey = $messageExt->getMsgId();
            }
            $endTransactionRequestHeader->setMsgId($uniqueKey);
            $endTransactionRequestHeader->setTransactionId($messageExt->getTransactionId());
            switch ($localTransactionState) {
                case LocalTransactionState::COMMIT_MESSAGE:
                    $endTransactionRequestHeader->setCommitOrRollback(MessageSysFlag::TRANSACTION_COMMIT_TYPE);
                    break;
                case LocalTransactionState::ROLLBACK_MESSAGE:
                    $endTransactionRequestHeader->setCommitOrRollback(MessageSysFlag::TRANSACTION_ROLLBACK_TYPE);
                    break;
                case LocalTransactionState::UNKNOW:
                    $endTransactionRequestHeader->setCommitOrRollback(MessageSysFlag::TRANSACTION_NOT_TYPE);
                    break;
                default:
                    break;
            }
            $this->log->info("check transaction state {} : {} - {}" , $localTransactionState , $messageExt->getMsgId() , $messageExt->getKeys());
            $request = RemotingCommand::createRequestCommand(RequestCode::$END_TRANSACTION, $endTransactionRequestHeader);
            $request->setRemark($remark);
            try{
                $client->send($request);
            }catch (\Exception $e){
                $this->log->error("reply transaction response message error:" . $e->getMessage());
            }
        }
    }

    function exception(\Exception $e)
    {
        // TODO: Implement exception() method.
    }


}