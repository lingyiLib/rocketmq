<?php
namespace lingyiLib\rocketmq\remoting;

class ClientLinkStatus
{
    const OFFLINE = -1;

    const WAIT = 0;

    const RECONNECT = 1;

    const CONNECTED = 2;
}