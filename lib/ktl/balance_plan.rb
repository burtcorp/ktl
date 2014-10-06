# encoding: utf-8

module Ktl
  class BalancePlan
    def initialize(zk_client, filter)
      @zk_client = zk_client
      @filter = Regexp.new(filter)
    end

    def generate
      all_topics = @zk_client.all_topics
      topics = all_topics.filter { |t| !!t.match(@filter) }
      topics_partitions = ScalaEnumerable.new(@zk_client.partitions_for_topics(topics))
      topics_partitions = topics_partitions.sort_by(&:_1)
      replica_assignments = @zk_client.replica_assignment_for_topics(topics)
      brokers = @zk_client.broker_ids
      reassignment_plan = Scala::Collection::Map.empty
      start_index = 0
      topics_partitions.each do |tp|
        topic = tp._1
        partitions = tp._2
        replicas = replica_assignments[Kafka::TopicAndPartition.new(topic, partitions.first)]
        assignment = Kafka::Admin.assign_replicas_to_brokers(brokers, partitions.size, replicas.size, start_index)
        assignment.foreach do |pr|
          topic_partition = Kafka::TopicAndPartition.new(topic, pr._1)
          reassignment_plan += Scala::Tuple.new(topic_partition, pr._2)
        end
        start_index = (start_index + 1) % brokers.size
      end
      reassignment_plan
    end
  end
end
