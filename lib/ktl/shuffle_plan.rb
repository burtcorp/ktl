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
      current_replica_assignments = @options[:current_assignment] || @zk_client.replica_assignment_for_topics(topics)
      brokers = select_brokers
      reassignment_plan = Scala::Collection::Map.empty
      topics_partitions.each do |tp|
        topic, partitions = tp.elements
        nr_replicas = @options[:replication_factor] || current_replica_assignments.apply(Kafka::TopicAndPartition.new(topic, 0)).size
        assignment = assign_replicas_to_brokers(topic, brokers, partitions.size, nr_replicas, current_replica_assignments)
        assignment.each do |pr|
          partition, replicas = pr.elements
          topic_partition = Kafka::TopicAndPartition.new(topic, partition)
          current_assignment = current_replica_assignments.apply(topic_partition)
          if current_assignment != replicas || include_all
            if @log_plan
              c = ScalaEnumerable.new(current_assignment).to_a
              r = ScalaEnumerable.new(replicas).to_a
              moving = r - c
              if moving.size > 0
                @logger.info "Moving #{topic_partition.topic},#{topic_partition.partition} from #{c} to #{r} (#{moving.size} new brokers)"
              else
                @logger.info "Reassigning #{topic_partition.topic},#{topic_partition.partition} from #{c} to #{r}"
              end
            end
            reassignment_plan += Scala::Tuple.new(topic_partition, replicas)
          end
        end
      end
      reassignment_plan
    end

    def generate_for_new_topic(topic, partition_count)
      brokers = select_brokers
      nr_replicas = @options[:replication_factor] || 1
      assignment = assign_replicas_to_brokers(topic, brokers, partition_count, nr_replicas, nil)
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

    def assign_replicas_to_brokers(topic, brokers, partition_count, replica_count, _)
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
    def assign_replicas_to_brokers(topic, brokers, partition_count, replica_count, _)
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
    end

    def assign_replicas_to_brokers(topic, brokers, partition_count, replica_count, _)
      if replica_count > brokers.size
        raise ArgumentError, sprintf('replication factor: %i larger than available brokers: %i', replica_count, brokers.size)
      end
      result = []
      racks = brokers.each_with_object({}) do |broker, acc|
        rack = rack_for(broker)
        acc[rack] ||= []
        acc[rack] << broker
      end
      partition_count.times do |partition|
        first_sorted = racks.flat_map do |rack, rack_brokers|
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

    private

    def rack_for(broker_id)
      @rack_mappings[broker_id] ||= Kafka::Admin.get_broker_rack(@zk_client, broker_id)
    end
  end

  class MinimalMovementShufflePlan < ShufflePlan
    def initialize(*args)
      super
      @rack_mappings = {}
      @leader_count = Hash.new(0)
    end

    def assign_replicas_to_brokers(topic, brokers, partition_count, replica_count, current_replica_assignments_for_topics)
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
      max_leader_per_broker = (partition_count / brokers.size.to_f).ceil
      partition_count.times do |partition|
        if current_replica_assignments_for_topics
          current_assignment = Scala::Collection::JavaConversions.as_java_iterable(current_replica_assignments_for_topics.apply(Kafka::TopicAndPartition.new(topic, partition))).to_a
        else
          current_assignment = []
        end
        rack_order = racks.keys.dup
        shift_leader_count = (partition % rack_order.length)
        move_leaders = rack_order.push(*rack_order.shift(shift_leader_count))
        rack_order = move_leaders.take(replica_count)

        rack_sorted_brokers = rack_order.map do |rack|
          rack_brokers = racks[rack].dup
          current_broker = current_assignment.select {|b| rack_for(b) == rack}.first
          active_broker = rack_brokers.delete(current_broker) if current_broker
          rack_brokers.shuffle!
          rack_brokers.unshift(current_broker) if active_broker
          rack_brokers
        end

        selected = rack_sorted_brokers.map do |rack_brokers|
          rack_selected_brokers = rack_brokers.select {|broker| broker_count[broker] < max_partitions_per_broker}
          rack_selected_brokers.first
        end.sort_by do |broker_id|
          @leader_count[broker_id]
        end
        selected.each do |allocated_broker|
          broker_count[allocated_broker] += 1
        end
        @leader_count[selected.first] += 1
        if selected.compact.length != replica_count
          raise ArgumentError, "Unable to find enough available brokers for #{topic} with #{partition_count} partitions and replica factor #{replica_count}. Max partitions per broker: #{max_partitions_per_broker}, current broker count: #{broker_count}, available brokers: #{brokers}"
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
