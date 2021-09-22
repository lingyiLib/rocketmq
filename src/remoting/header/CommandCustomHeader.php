<?php
namespace lingyiLib\rocketmq\remoting\header;

interface CommandCustomHeader
{
    /**
     * @return array
     */
    function getHeader();
}