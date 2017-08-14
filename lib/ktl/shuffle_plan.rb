# encoding: utf-8

module Ktl
  class ShufflePlan
    def initialize(zk_client, options = {})
      @zk_client = zk_client
      @options = options
      @logger = options[:logger] || NullLogger.new
      @log_plan = !!options[:log_plan]
    end

    def generate(include_all = false)
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
          if current_assignment != replicas || include_all
            @logger.info "Moving #{topic_partition.topic},#{topic_partition.partition} from #{current_assignment} to #{replicas}" if @log_plan
            reassignment_plan += Scala::Tuple.new(topic_partition, replicas)
          end
        end
      end
      reassignment_plan
    end

    def generate_for_new_topic(topic, partition_count)
      brokers = select_brokers
      nr_replicas = @options[:replication_factor] || 1
      assignment = assign_replicas_to_brokers(topic, brokers, partition_count, nr_replicas)
      assignment.map do |pr|
        partition, replicas = pr.elements
        Scala::Collection::JavaConversions.as_java_iterable(replicas).to_a
      end
    end

    private

    def select_brokers
      brokers = @options[:brokers] ? Array(@options[:brokers]).map(&:to_i) : ScalaEnumerable.new(@zk_client.broker_ids).to_a
      brokers -= Array(@options[:blacklist]).map(&:to_i) if @options[:blacklist]
      brokers
    end

    def assign_replicas_to_brokers(topic, brokers, partition_count, replica_count)
      @broker_metadatas ||= begin
        broker_metadatas = Kafka::Admin.get_broker_metadatas(@zk_client, brokers)
        Scala::Collection::JavaConversions.as_scala_iterable(broker_metadatas).to_seq
      end
      Kafka::Admin.assign_replicas_to_brokers(@broker_metadatas, partition_count, replica_count)
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
    def initialize(*args)
      super
      @rack_mappings = {}
      @leader_count = Hash.new(0)
    end

    def assign_replicas_to_brokers(topic, brokers, partition_count, replica_count)
      if replica_count > brokers.size
        raise ArgumentError, sprintf('replication factor: %i larger than available brokers: %i', replica_count, brokers.size)
      end
      result = []
      racks = brokers.each_with_object({}) do |broker, acc|
        rack = rack_for(broker)
        acc[rack] ||= []
        acc[rack] << broker
      end
      broker_count = Hash.new(0)
      max_partitions_per_broker = ((partition_count * replica_count) / brokers.size.to_f).ceil
      partition_count.times do |partition|
        rack_order = racks.keys.dup
        shift_leader_count = (partition % rack_order.length)
        move_leaders = rack_order.push(*rack_order.shift(shift_leader_count))
        rack_order = rack_order.take(replica_count)
        rack_sorted_brokers = rack_order.map do |rack|
          racks[rack].sort_by do |broker|
            key = [partition, topic, broker].pack('l<a*l<')
            Java::OrgJrubyUtil::MurmurHash.hash32(key.to_java_bytes, 0, key.bytesize, SEED)
          end
        end
        selected = rack_sorted_brokers.map do |rack_brokers|
          rack_selected_broker = rack_brokers.select {|broker| broker_count[broker] < max_partitions_per_broker}.first
        end.sort_by do |broker_id|
          @leader_count[broker_id]
        end
        selected.each do |allocated_broker|
          broker_count[allocated_broker] += 1
        end
        @leader_count[selected.first] += 1
        if selected.compact.length != replica_count
          raise ArgumentError, "Unable to find enough available brokers "
        end
        result.push(Scala::Tuple.new(partition, Scala::Collection::JavaConversions.as_scala_iterable(selected).to_list))
      end
      result
    end

    private

    def rack_for(broker_id)
      @rack_mappings[broker_id] ||= Kafka::Admin.get_broker_rack(@zk_client, broker_id)
    end
  end
end
