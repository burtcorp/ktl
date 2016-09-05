# encoding: utf-8

module Ktl
  class ShufflePlan
    def initialize(zk_client, options = {})
      @zk_client = zk_client
      @options = options
    end

    def generate
      topics = @zk_client.all_topics
      if (filter = @options[:filter])
        topics = topics.filter { |t| !!t.match(filter) }
      end
      topics_partitions = ScalaEnumerable.new(@zk_client.partitions_for_topics(topics))
      topics_partitions = topics_partitions.sort_by(&:first)
      replica_assignments = @zk_client.replica_assignment_for_topics(topics)
      brokers = select_brokers
      reassignment_plan = Scala::Collection::Map.empty
      topics_partitions.each do |tp|
        topic, partitions = tp.elements
        nr_replicas = @options[:replication_factor] || replica_assignments.apply(Kafka::TopicAndPartition.new(topic, 0)).size
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
      brokers = @options[:brokers] ? Array(@options[:brokers]).map(&:to_i) : ScalaEnumerable.new(@zk_client.broker_ids).to_a
      brokers -= Array(@options[:blacklist]).map(&:to_i) if @options[:blacklist]
      brokers
    end

    def assign_replicas_to_brokers(topic, brokers, partition_count, replica_count)
      broker_metadatas = brokers.map { |x| Kafka::Admin::BrokerMetadata.new(x.to_java(:int), Scala::Option[nil]) }
      broker_metadatas = Scala::Collection::JavaConversions.as_scala_iterable(broker_metadatas).to_seq
      Kafka::Admin.assign_replicas_to_brokers(broker_metadatas, partition_count, replica_count)
    rescue Kafka::Admin::AdminOperationException => e
      raise ArgumentError, sprintf('%s (%s)', e.message, e.class.name), e.backtrace
    end
  end

  class RendezvousShufflePlan < ShufflePlan
    def assign_replicas_to_brokers(topic, brokers, partition_count, replica_count)
      if replica_count > brokers.size
        raise ArgumentError, sprintf('replication factor: %i larger than available brokers: %i', replica_count, brokers.size)
      end
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

  class RackAwareShufflePlan < RendezvousShufflePlan
    def assign_replicas_to_brokers(topic, brokers, partition_count, replica_count)
      if replica_count > brokers.size
        raise ArgumentError, sprintf('replication factor: %i larger than available brokers: %i', replica_count, brokers.size)
      end

      broker_metadatas = Kafka::Admin::AdminUtils.get_broker_metadatas(@zk_client.utils, Kafka::Admin::RackAwareMode[1], Scala::Option[Scala::Collection::JavaConversions.as_scala_iterable(brokers).to_list])
      broker_metadatas = Scala::Collection::JavaConversions.seq_as_java_list(broker_metadatas).to_a
      racks = Hash.new { |hash, key| hash[key] = [] }
      brokers = broker_metadatas.each do |bm|
        rack = bm.rack.getOrElse(nil)
        racks[rack] << bm.id
      end

      result = []
      partition_count.times do |partition|
        first_sorted = racks.each.flat_map do |rack, rack_brokers|
          hashed_brokers = rack_brokers.map do |broker|
            key = [partition, topic, broker].pack('l<a*l<')
            {id: broker, hash: Java::OrgJrubyUtil::MurmurHash.hash32(key.to_java_bytes, 0, key.bytesize, SEED)}
          end.sort_by do |broker|
            broker[:hash]
          end
          hashed_brokers.each_with_index do |broker, index|
            broker[:index] = index
          end
        end
        sorted = first_sorted.sort_by do |broker|
          [broker[:index], broker[:hash], broker[:id]]
        end
        selected = sorted.take(replica_count).map {|broker| broker[:id]}
        result.push(Scala::Tuple.new(partition, Scala::Collection::JavaConversions.as_scala_iterable(selected).to_list))
      end
      result
    end
  end
end
