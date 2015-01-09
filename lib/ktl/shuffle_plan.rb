# encoding: utf-8

module Ktl
  class ShufflePlan
    def initialize(zk_client, filter)
      @zk_client = zk_client
      @filter = Regexp.new(filter)
    end

    def generate
      topics = @zk_client.all_topics
      topics = topics.filter { |t| !!t.match(@filter) }
      topics_partitions = ScalaEnumerable.new(@zk_client.partitions_for_topics(topics))
      topics_partitions = topics_partitions.sort_by(&:first)
      replica_assignments = @zk_client.replica_assignment_for_topics(topics)
      brokers = @zk_client.broker_ids
      reassignment_plan = Scala::Collection::Map.empty
      topics_partitions.each do |tp|
        topic, partitions = tp.elements
        nr_replicas = replica_assignments.apply(Kafka::TopicAndPartition.new(topic, 0)).size
        assignment = Kafka::Admin.assign_replicas_to_brokers(brokers, partitions.size, nr_replicas)
        assignment.each do |pr|
          partition, replicas = pr.elements
          topic_partition = Kafka::TopicAndPartition.new(topic, partition)
          current_assignment = replica_assignments.apply(topic_partition)
          unless current_assignment == replicas
            reassignment_plan += Scala::Tuple.new(topic_partition, replicas)
          end
        end
      end
      reassignment_plan
    end
  end
end
