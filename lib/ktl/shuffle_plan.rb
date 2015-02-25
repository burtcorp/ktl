# encoding: utf-8

module Ktl
  class ShufflePlan
    def initialize(zk_client, filter, options = {})
      @zk_client = zk_client
      @filter = filter
      @options = options
    end

    def generate
      topics = @zk_client.all_topics
      topics = topics.filter { |t| !!t.match(@filter) }
      topics_partitions = ScalaEnumerable.new(@zk_client.partitions_for_topics(topics))
      topics_partitions = topics_partitions.sort_by(&:first)
      replica_assignments = @zk_client.replica_assignment_for_topics(topics)
      brokers = select_brokers
      reassignment_plan = Scala::Collection::Map.empty
      topics_partitions.each do |tp|
        topic, partitions = tp.elements
        nr_replicas = replica_assignments.apply(Kafka::TopicAndPartition.new(topic, 0)).size
        assignment = assign_replicas_to_brokers(topic, brokers, partitions.size, nr_replicas)
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

    private

    def select_brokers
      brokers = @options[:brokers] ? Array(@options[:brokers]) : ScalaEnumerable.new(@zk_client.broker_ids).to_a
      brokers -= Array(@options[:blacklist]) if @options[:blacklist]
      brokers
    end

    def assign_replicas_to_brokers(topic, brokers, partition_count, replica_count)
      brokers = Scala::Collection::JavaConversions.as_scala_iterable(brokers.map { |x| x.to_java(:int) }).to_list
      Kafka::Admin.assign_replicas_to_brokers(brokers, partition_count, replica_count)
    end
  end

  class RendezvousShufflePlan < ShufflePlan
    def assign_replicas_to_brokers(topic, brokers, partition_count, replica_count)
      result = []
      partition_count.times do |partition|
        sorted = brokers.sort_by do |broker|
          key = [partition, topic, broker].pack('l<a*l<')
          Java::OrgJrubyUtil::MurmurHash.hash32(key.to_java_bytes, 0, key.bytesize, SEED)
        end
        selected = sorted.take(replica_count)
        result.push(Scala::Tuple.new(partition, Scala::Collection::JavaConversions.as_scala_iterable(selected).to_list))
      end
      result
    end

    private

    SEED = 1683520333
  end
end
