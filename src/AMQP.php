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


        public function __construct(array $parameters = [])
        {
            $this->assign($parameters);

            $connection = new AMQPStreamConnection($this->host, $this->port, $this->user, $this->pass);
            $this->channel = $connection->channel();
        }

        public function wait(string $from, callable $callback)
        {
            $handler = function (AMQPMessage $message) use ($callback) {
                $response = call_user_func($callback, $message->getBody());

                if ($message->has('reply_to')) {
                    $response = new AMQPMessage($response, ['correlation_id' => $message->get('correlation_id')]);
                    $message->delivery_info['channel']->basic_publish($response, '', $message->get('reply_to'));
                }
            };

            $this->channel->queue_declare($from, false, false, false, false);
            $this->channel->basic_consume($from, '', false, true, false, false, $handler);

            while(count($this->channel->callbacks)) {
                $this->channel->wait();
            }
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
                    $response = $rep->body;
                }
            };

            list($queue, ,) = $this->channel->queue_declare('', false, false, true, false);
            $this->channel->basic_consume($queue, '', false, false, false, false, $handler);

            $msg = new AMQPMessage($message, [
                'correlation_id' => $correlationId,
                'reply_to' => $queue
            ]);

            $this->channel->basic_publish($msg, '', $to);

            while(!$response) {
                $this->channel->wait();
            }

            return $response;
        }
    }
}
