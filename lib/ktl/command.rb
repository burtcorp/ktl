# encoding: utf-8

module Ktl
  class Command < Thor

    private

    def with_zk_client
      zk_client = ZookeeperClient.new(options.zookeeper).setup
      yield zk_client
    rescue => e
      say 'Error: %s (%s)' % [e.message, e.class.name], :red
      say e.backtrace.join($/)
    ensure
      zk_client.close if zk_client
    end

    def with_kafka_client(options={})
      brokers = with_zk_client { |zk_client| ScalaEnumerable.new(zk_client.brokers).map(&:connection_string) }
      kafka_client = KafkaClient.create(options.merge(hosts: brokers))
      yield kafka_client
    ensure
      kafka_client.close if kafka_client
    end
  end
end
