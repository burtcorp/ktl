# encoding: utf-8

require 'kafka-jars'


module Log4j
  include_package 'org.apache.log4j'

  BasicConfigurator.configure
  Logger.root_logger.set_level(Level::ERROR)
end

java_import 'org.I0Itec.zkclient.ZkClient'


module Scala
  java_import 'scala.Tuple2'

  Tuple = Tuple2

  module Collection
    include_package 'scala.collection'

    module Immutable
      include_package 'scala.collection.immutable'
    end
  end
end

module Kafka
  module Utils
    include_package 'kafka.utils'

    def self.new_zk_client(zk_connect, timeout=30_000)
      ::ZkClient.new(zk_connect, timeout, timeout, ZKStringSerializer)
    end

    def self.get_partitions_for_topic(zk, topic)
      topics = Scala::Collection::Immutable::List.from_array([topic].to_java)
      partitions = ZkUtils.get_partitions_for_topics(zk, topics)
      partitions.get(topic).get
    end
  end

  module Admin
    include_package 'kafka.admin'

    TopicCommandOptions = TopicCommand::TopicCommandOptions

    def self.to_topic_options(hash)
      options = hash.flat_map do |key, value|
        ['--' + key.to_s.gsub('_', '-'), value].compact
      end
      TopicCommandOptions.new(options)
    end

    def self.reassign_partitions(zk_client, migration_plan)
      command = ReassignPartitionsCommand.new(zk_client, migration_plan)
      command.reassign_partitions
    end
  end

  module Common
    include_package 'kafka.common'
  end

  TopicAndPartition = Common::TopicAndPartition
end
