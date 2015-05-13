# encoding: utf-8

module Kafka
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
end
