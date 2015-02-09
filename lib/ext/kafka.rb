# encoding: utf-8

require 'kafka-jars'


module Log4j
  include_package 'org.apache.log4j'

  BasicConfigurator.configure
  Logger.root_logger.set_level(Level::ERROR)
end

module ZkClient
  java_import 'org.I0Itec.zkclient.ZkClient'

  module Exception
    include_package 'org.I0Itec.zkclient.exception'
  end
end

module Scala
  java_import 'scala.Tuple2'

  class Tuple2
    alias_method :first, :_1
    alias_method :last, :_2

    def elements
      [first, last]
    end
  end
  Tuple = Tuple2

  module Collection
    include_package 'scala.collection'

    module Mutable
      include_package 'scala.collection.mutable'
    end

    module Immutable
      include_package 'scala.collection.immutable'
    end
  end
end

class ScalaEnumerable
  include Enumerable

  def initialize(underlying)
    @underlying = underlying
  end

  def each(&block)
    @underlying.foreach(&block)
  end
end

module Kafka
  module Utils
    include_package 'kafka.utils'

    def self.new_zk_client(zk_connect, timeout=30_000)
      ::ZkClient::ZkClient.new(zk_connect, timeout, timeout, ZKStringSerializer)
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

    def self.preferred_replica(zk_client, topics_partitions)
      PreferredReplicaLeaderElectionCommand.write_preferred_replica_election_data(zk_client, topics_partitions)
    end

    def self.assign_replicas_to_brokers(brokers, partitions, repl_factor, index=-1, partition=-1)
      assignment = AdminUtils.assign_replicas_to_brokers(brokers, partitions, repl_factor, index, partition)
      ScalaEnumerable.new(assignment)
    end
  end

  module Common
    include_package 'kafka.common'
  end

  TopicAndPartition = Common::TopicAndPartition

  module Tools
    include_package 'kafka.tools'
  end
end
