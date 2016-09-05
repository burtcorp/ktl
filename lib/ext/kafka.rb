# encoding: utf-8

require 'kafka-jars'
require 'log4j-jars'

module Log4j
  include_package 'org.apache.log4j'
  java_import 'org.apache.log4j.Logger'

  BasicConfigurator.configure
  org.apache.log4j.Logger.root_logger.set_level(Level::ERROR)
end

module ZkClient
  java_import 'org.I0Itec.zkclient.ZkClient'

  module Exception
    include_package 'org.I0Itec.zkclient.exception'
  end
end

module Scala
  java_import 'scala.Console'
  java_import 'scala.Tuple2'
  java_import 'scala.Option'

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
      partitions = zk.get_partitions_for_topics(topics)
      partitions.get(topic).get
    end

    def self.delete_topic(zk, topic)
      acl = Kafka::Utils::ZkUtils::DefaultAcls(false)
      zk.create_persistent_path(ZkUtils.get_delete_topic_path(topic), '', acl)
    end
  end

  module Api
    include_package 'kafka.api'
  end

  module Cluster
    include_package 'kafka.cluster'
  end

  module Admin
    include_package 'kafka.admin'

    TopicCommandOptions = TopicCommand::TopicCommandOptions

    def self.to_topic_options(hash)
      options = hash.flat_map do |key, value|
        kafka_key = '--' + key.to_s.gsub('_', '-')
        if value.is_a?(Hash)
          value.map { |k, v| [kafka_key, [k, v].join('=')] }
        elsif value.is_a?(Array)
          value.map { |v| [kafka_key, v] }
        else
          [kafka_key, value].compact
        end
      end
      TopicCommandOptions.new(options.flatten)
    end

    def self.preferred_replica(zk_client, topics_partitions)
      PreferredReplicaLeaderElectionCommand.write_preferred_replica_election_data(zk_client, topics_partitions)
    end

    def self.assign_replicas_to_brokers(brokers, partitions, repl_factor, index=-1, partition=-1)
      assignment = AdminUtils.assign_replicas_to_brokers(brokers, partitions.to_java(:int), repl_factor.to_java(:int), index.to_java(:int), partition.to_java(:int))
      ScalaEnumerable.new(assignment)
    end

    def self.get_broker_metadatas(zk_client, brokers)
      broker_metadatas = Kafka::Admin::AdminUtils.get_broker_metadatas(zk_client.utils, Kafka::Admin::RackAwareMode[1], Scala::Option[Scala::Collection::JavaConversions.as_scala_iterable(brokers).to_list])
      Scala::Collection::JavaConversions.seq_as_java_list(broker_metadatas).to_a
    end
  end

  module Protocol
    java_import 'org.apache.kafka.common.protocol.SecurityProtocol'
  end

  module Common
    include_package 'kafka.common'
  end

  TopicAndPartition = Common::TopicAndPartition

  module Tools
    include_package 'kafka.tools'
  end
end
