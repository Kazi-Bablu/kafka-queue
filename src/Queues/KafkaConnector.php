<?php

namespace Hp\KafkaQueue\Queues;

use Illuminate\Database\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{
    public function connect(array $config)
    {
        // $conf = new \Rdkafka\Conf();
        // $conf->set('bootstrap.servers', 'pkc-4r087.us-west2.gcp.confluent.cloud:9092');
        // $conf->set('security.protocol', 'SASL_SSL');
        // $conf->set('sasl.mechanism', 'PLAIN');
        // $conf->set('sasl.username', 'P6BM4UYTNUY7IXKK');
        // $conf->set('sasl.password', 'YaVXrjhbfGavHWZKOrnP0CholU9TePHxvHFEHr2pgZ7CpvYPeqPj1UaIq38B/sVz');
        // $conf->set('group.id', 'myGroup');
        // $conf->set('auto.offset.reset', 'earliest');

        $conf = new \Rdkafka\Conf();
        $conf->set('bootstrap.servers', $config['bootstrap_servers']);
        $conf->set('security.protocol', $config['security_protocol']);
        $conf->set('sasl.mechanism', $config['sasl_mechanisms']);
        $conf->set('sasl.username', $config['sasl_username']);
        $conf->set('sasl.password', $config['sasl_password']);

        $producer = new \RdKafka\Producer($conf);

        $conf->set('group.id', $config['group_id']);
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new \RdKafka\KafkaConsumer($conf);

        return new KafkaQueue($producer, $consumer);
    }
}
