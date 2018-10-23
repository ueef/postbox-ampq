<?php

namespace Ueef\Postbox\Drivers {

    use PhpAmqpLib\Message\AMQPMessage;
    use PhpAmqpLib\Channel\AMQPChannel;
    use PhpAmqpLib\Connection\AMQPStreamConnection;
    use Ueef\Postbox\Interfaces\DriverInterface;
    use Ueef\Assignable\Traits\AssignableTrait;
    use Ueef\Assignable\Interfaces\AssignableInterface;

    class AMQP implements AssignableInterface, DriverInterface
    {
        use AssignableTrait;

        /**
         * @var string
         */
        private $host = 'localhost';

        /**
         * @var integer
         */
        private $port = 5672;

        /**
         * @var string
         */
        private $user = 'guest';

        /**
         * @var string
         */
        private $pass = 'guest';

        /**
         * @var AMQPChannel
         */
        private $channel;

        private $canSafelyExit = true;

        private $mustExit = false;

        /**
         * @var string
         */
        private $callbackQueue;

        public function __construct(array $parameters = [])
        {
            $this->assign($parameters);

            $connection = new AMQPStreamConnection($this->host, $this->port, $this->user, $this->pass);
            $this->channel = $connection->channel();
        }

        public function wait() {
            while(count($this->channel->callbacks)) {
                $this->channel->wait();
            }
        }

        public function consume(string $from, callable $callback)
        {
            $this->registerSignalHandlers();
            $handler = function (AMQPMessage $message) use ($callback) {
                $response = call_user_func($callback, $message->getBody());

                if ($message->has('reply_to')) {
                    $response = new AMQPMessage($response, ['correlation_id' => $message->get('correlation_id')]);
                    $message->delivery_info['channel']->basic_publish($response, '', $message->get('reply_to'));
                }

                $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);

                if ($this->mustExit) {
                    exit(0);
                }
            };

            $this->channel->queue_declare($from, false, false, false, false);
            $this->channel->basic_qos(null, 1, null);
            $this->channel->basic_consume($from, '', false, false, false, false, $handler);
        }

        public function send(string $to, string $message)
        {
            $this->channel->basic_publish(new AMQPMessage($message), '', $to);
        }

        public function request(string $to, string $message): string
        {
            $response = null;
            $correlationId = uniqid();

            $handler = function (AMQPMessage $rep) use (&$response, $correlationId) {
                if($rep->get('correlation_id') == $correlationId) {

                    $rep->delivery_info['channel']->basic_ack($rep->delivery_info['delivery_tag']);
                    //удаление текущего обработчика из канала
                    $rep->delivery_info['channel']->basic_cancel($rep->delivery_info['consumer_tag']);

                    $response = $rep->body;
                }
            };

            if (empty($this->callbackQueue)) {
                list($this->callbackQueue, ,) = $this->channel->queue_declare('', false, false, true, false);
            }
            $this->channel->basic_consume($this->callbackQueue, '', false, false, false, false, $handler);

            $msg = new AMQPMessage($message, [
                'correlation_id' => $correlationId,
                'reply_to' => $this->callbackQueue
            ]);

            $this->channel->basic_publish($msg, '', $to);

            $this->canSafelyExit = false;
            while(!$response) {
                $this->channel->wait();
            }
            $this->canSafelyExit = true;

            return $response;
        }

        private function registerSignalHandlers()
        {
            $signalHandler = function () {
                if ($this->canSafelyExit) {
                    exit(0);
                }

                $this->mustExit = true;
            };

            pcntl_signal(SIGINT, $signalHandler);
            pcntl_signal(SIGTERM, $signalHandler);
        }
    }
}
