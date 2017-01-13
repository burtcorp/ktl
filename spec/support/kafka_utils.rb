# encoding: utf-8

require 'slf4j-jars'
require 'kafka-jars'


module Kafka
  module JavaApi
    java_import 'kafka.javaapi.TopicMetadata'
    java_import 'kafka.javaapi.TopicMetadataRequest'
    java_import 'kafka.javaapi.TopicMetadataResponse'
  end

  module Consumer
    include_package 'kafka.consumer'
    java_import 'kafka.javaapi.consumer.SimpleConsumer'
  end

  module Metrics
    java_import 'kafka.metrics.KafkaMetricsGroup'

    module KafkaMetricsGroup
      def self.remove_all_consumer_metrics(client_id)
        self.removeAllConsumerMetrics(client_id)
      end
    end
  end

  module Test
    java_import 'java.net.InetSocketAddress'
    java_import 'org.apache.zookeeper.server.ZooKeeperServer'
    java_import 'org.apache.zookeeper.server.NIOServerCnxnFactory'
    java_import 'kafka.server.KafkaServerStartable'
    java_import 'kafka.server.KafkaConfig'

    def self.create_zk_server(connect_string)
      EmbeddedZookeeper.new(connect_string)
    end

    def self.create_kafka_server(config)
      EmbeddedKafkaServer.new(config)
    end

    class EmbeddedZookeeper
      def initialize(connect_string)
        @connect_string = connect_string
        @snapshot_dir = Dir.mktmpdir
        @log_dir = Dir.mktmpdir
        @zookeeper = ZooKeeperServer.new(java.io.File.new(@snapshot_dir), java.io.File.new(@log_dir), 500.to_java(:int))
        port = connect_string.split(':').last.to_i
        @factory = NIOServerCnxnFactory.new
        @factory.configure(InetSocketAddress.new('127.0.0.1', port), 0)
      end

      def start
        @factory.startup(@zookeeper)
      end

      def shutdown
        @zookeeper.shutdown
        @factory.shutdown
      ensure
        FileUtils.remove_entry_secure(@snapshot_dir)
        FileUtils.remove_entry_secure(@log_dir)
      end
    end

    class EmbeddedKafkaServer
      def initialize(config)
        @log_dir = Dir.mktmpdir
        properties = java.util.Properties.new
        config.each do |key, value|
          properties.put(key, value.to_s)
        end
        properties.put('log.dirs', @log_dir)
        properties.put('controlled.shutdown.enable', 'false')
        @server = KafkaServerStartable.new(KafkaConfig.new(properties))
      end

      def start
        @server.startup
      end

      def shutdown
        @server.shutdown
      ensure
        FileUtils.remove_entry_secure(@log_dir)
      end
    end
  end

  class TopicMetadataResponse
    include Enumerable

    def initialize(underlying)
      @underlying = underlying

      @cache = Hash.new.tap do |h|
        CACHES.each do |type|
          h[type] = Hash.new({})
        end
      end
    end

    def each(&block)
      metadata.each do |topic_metadata|
        topic_metadata.partitions_metadata.each do |partition_metadata|
          yield topic_metadata.topic, partition_metadata
        end
      end
    end

    def metadata
      @underlying.topics_metadata
    end

    def leader_for(topic, partition)
      with_cache(:leader, topic, partition)
    end

    def isr_for(topic, partition)
      with_cache(:isr, topic, partition)
    end
    alias_method :in_sync_replicas_for, :isr_for

    private

    CACHES = [:leader, :isr].freeze

    def with_cache(type, topic, partition)
      return @cache[type][topic][partition] if @cache[type][topic][partition]

      partition_metadata = locate_partition_metadata(topic, partition)

      if partition_metadata
        @cache[type][topic][partition] = partition_metadata.send(type)
      else
        raise NoSuchTopicPartitionCombinationError, "Cannot find (#{topic}:#{partition}) combination"
      end
    end

    def locate_partition_metadata(topic, partition)
      metadata.each do |tm|
        if tm.topic == topic
          tm.partitions_metadata.each do |pm|
            return pm if pm.partition_id == partition
          end
        end
      end

      nil
    end
  end
end
