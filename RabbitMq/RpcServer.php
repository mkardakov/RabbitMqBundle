<?php

namespace OldSound\RabbitMqBundle\RabbitMq;

use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * Rpc server
 */
class RpcServer extends BaseConsumer
{
    const TIMEOUT_TYPE_GRACEFUL_MAX_EXECUTION = 'graceful-max-execution';
    const TIMEOUT_TYPE_IDLE                   = 'idle';

    /**
     * DateTime after which the consumer will gracefully exit. "Gracefully" means, that
     * any currently running consumption will not be interrupted.
     *
     * @var \DateTime|null
     */
    protected $gracefulMaxExecutionDateTime;

    /**
     * Exit code used, when consumer is closed by the Graceful Max Execution Timeout feature.
     *
     * @var int
     */
    protected $gracefulMaxExecutionTimeoutExitCode = 0;

    /**
     * Serializer
     *
     * @var string
     */
    protected $serializer = 'serialize';

    /**
     * Set graceful max execution date time
     *
     * @param \DateTime $gracefulMaxExecutionDateTime
     *
     * @return RpcServer
     */
    public function setGracefulMaxExecutionDateTime(\DateTime $gracefulMaxExecutionDateTime)
    {
        $this->gracefulMaxExecutionDateTime = $gracefulMaxExecutionDateTime;
        return $this;
    }

    /**
     * Set graceful max execution date time from seconds in the future
     *
     * @param int $secondsInTheFuture
     *
     * @return $this
     */
    public function setGracefulMaxExecutionDateTimeFromSecondsInTheFuture($secondsInTheFuture)
    {
        $this->setGracefulMaxExecutionDateTime(new \DateTime("+{$secondsInTheFuture} seconds"));
        return $this;
    }

    /**
     * Set graceful max execution timeout exit code
     *
     * @param int $gracefulMaxExecutionTimeoutExitCode
     *
     * @return RpcServer
     */
    public function setGracefulMaxExecutionTimeoutExitCode($gracefulMaxExecutionTimeoutExitCode)
    {
        $this->gracefulMaxExecutionTimeoutExitCode = $gracefulMaxExecutionTimeoutExitCode;
        return $this;
    }

    /**
     * Start
     *
     * @param int $msgAmount
     *
     * @return int
     */
    public function start($msgAmount = 0)
    {
        $this->target = $msgAmount;

        $this->setupConsumer();

        while (count($this->getChannel()->callbacks)) {

            $this->maybeStopConsumer();

            /*
             * Be careful not to trigger ::wait() with 0 or less seconds, when
             * graceful max execution timeout is being used.
             */
            $waitTimeout = $this->chooseWaitTimeout();

            if ($waitTimeout['timeoutType'] === self::TIMEOUT_TYPE_GRACEFUL_MAX_EXECUTION
                && $waitTimeout['seconds'] < 1
            ) {
                return $this->gracefulMaxExecutionTimeoutExitCode;
            }

            try {
                $this->getChannel()->wait(null, false, $waitTimeout['seconds']);
            } catch (AMQPTimeoutException $e) {
                $this->forceStop = true;
            }
        }

        return 0;
    }

    /**
     * Init server
     *
     * @param string $name
     *
     * @return void
     */
    public function initServer($name)
    {
        $this->setExchangeOptions(array('name' => $name, 'type' => 'direct'));
        $this->setQueueOptions(array('name' => $name . '-queue'));
    }

    /**
     * Process message
     *
     * @param AMQPMessage $msg
     *
     * @return void
     */
    public function processMessage(AMQPMessage $msg)
    {
        try {
            $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
            $result = call_user_func($this->callback, $msg);
            $result = call_user_func($this->serializer, $result);
            $this->sendReply($result, $msg->get('reply_to'), $msg->get('correlation_id'));
            $this->consumed++;
            $this->maybeStopConsumer();
        } catch (\Exception $e) {
            $this->sendReply('error: ' . $e->getMessage(), $msg->get('reply_to'), $msg->get('correlation_id'));
        }
    }

    /**
     * Send reply
     *
     * @param string $result
     * @param string $client
     * @param mixed  $correlationId
     *
     * @return void
     */
    protected function sendReply($result, $client, $correlationId)
    {
        $reply = new AMQPMessage(
            $result,
            array(
                'content_type'   => 'text/plain',
                'correlation_id' => $correlationId
            )
        );
        $this->getChannel()->basic_publish($reply, '', $client);
    }

    /**
     * Set serializer
     *
     * @param mixed $serializer
     *
     * @return void
     */
    public function setSerializer($serializer)
    {
        $this->serializer = $serializer;
    }

    /**
     * Choose the timeout to use for the $this->getChannel()->wait() method.
     *
     * @return array Of structure
     *  {
     *      timeoutType: string; // one of self::TIMEOUT_TYPE_*
     *      seconds: int;
     *  }
     */
    private function chooseWaitTimeout()
    {
        if ($this->gracefulMaxExecutionDateTime) {
            $allowedExecutionDateInterval = $this->gracefulMaxExecutionDateTime->diff(new \DateTime());
            $allowedExecutionSeconds =  $allowedExecutionDateInterval->days * 86400
                + $allowedExecutionDateInterval->h * 3600
                + $allowedExecutionDateInterval->i * 60
                + $allowedExecutionDateInterval->s;

            if (!$allowedExecutionDateInterval->invert) {
                $allowedExecutionSeconds *= -1;
            }

            /*
             * Respect the idle timeout if it's set and if it's less than
             * the remaining allowed execution.
             */
            if ($this->getIdleTimeout()
                && $this->getIdleTimeout() < $allowedExecutionSeconds
            ) {
                return array(
                    'timeoutType' => self::TIMEOUT_TYPE_IDLE,
                    'seconds'     => $this->getIdleTimeout(),
                );
            }

            return array(
                'timeoutType' => self::TIMEOUT_TYPE_GRACEFUL_MAX_EXECUTION,
                'seconds'     => $allowedExecutionSeconds,
            );
        }

        return array(
            'timeoutType' => self::TIMEOUT_TYPE_IDLE,
            'seconds'     => $this->getIdleTimeout(),
        );
    }
}
