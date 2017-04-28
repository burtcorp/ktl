# encoding: utf-8

module Ktl
  class Reassigner

    attr_reader :limit

    def initialize(zk_client, options={})
      @zk_client = zk_client
      @limit = options[:limit]
      @overflow_path = '/ktl/overflow'
      @state_path = '/ktl/reassign'
      @logger = options[:logger] || NullLogger.new
      @log_assignments = !!options[:log_assignments]
    end

    def reassignment_in_progress?
      partitions = @zk_client.partitions_being_reassigned
      partitions.size > 0
    end

    def overflow?
      overflow_znodes = @zk_client.get_children(@overflow_path)
      overflow_znodes.size > 0
    rescue ZkClient::Exception::ZkNoNodeException
      false
    end

    def load_overflow
      overflow = Scala::Collection::Map.empty
      overflow_nodes = @zk_client.get_children(@overflow_path)
      overflow_nodes.foreach do |index|
        overflow_json = @zk_client.read_data(overflow_path(index)).first
        data = parse_reassignment_json(overflow_json)
        overflow = overflow.send('++', data)
      end
      overflow
    rescue ZkClient::Exception::ZkNoNodeException
      Scala::Collection::Map.empty
    end

    def execute(reassignment)
      reassign(reassignment)
    end

    private

    JSON_MAX_SIZE = 1024**2

    def reassign(reassignment)
      if (limit)
        @logger.info 'reassigning %d of %d partitions' % [limit, reassignment.size]
      else
        @logger.info 'reassigning %d partitions' % reassignment.size
      end

      reassignments = split(reassignment, @limit)
      reassignment_candidates = reassignments.shift
      actual_reassignment = Scala::Collection::Map.empty
      next_step_assignments = Scala::Collection::Map.empty
      Scala::Collection::JavaConversions.as_java_iterable(reassignment_candidates).each do |pr|
        topic_and_partition, replicas = pr.elements
        if step1_replicas = is_two_step_operation(topic_and_partition, replicas)
          next_step_assignments += pr
          actual_reassignment += Scala::Tuple.new(topic_and_partition, step1_replicas)
          brokers = Scala::Collection::JavaConversions.as_java_iterable(step1_replicas).to_a
          eventual_brokers = Scala::Collection::JavaConversions.as_java_iterable(replicas).to_a
          @logger.debug "Mirroring #{topic_and_partition.topic},#{topic_and_partition.partition} to #{brokers.join(',')} for eventual transition to #{eventual_brokers.join(',')}" if @log_assignments
        else
          actual_reassignment += pr
          brokers = Scala::Collection::JavaConversions.as_java_iterable(replicas).to_a
          @logger.debug "Assigning #{topic_and_partition.topic},#{topic_and_partition.partition} to #{brokers.join(',')}" if @log_assignments
        end
      end
      json = reassignment_json(actual_reassignment)
      @zk_client.reassign_partitions(json)
      manage_overflow(split(next_step_assignments, nil) + reassignments)
      manage_progress_state(actual_reassignment)
    end

    def is_two_step_operation(topic_and_partition, final_replicas)
      replicas = Scala::Collection::JavaConversions.as_java_iterable(final_replicas).to_a
      topic_list = Scala::Collection::JavaConversions.as_scala_iterable([topic_and_partition.topic])
      assignments = ScalaEnumerable.new(@zk_client.replica_assignment_for_topics(topic_list))
      assignments.each do |item|
        item_topic_partition = item.first
        if item_topic_partition.partition == topic_and_partition.partition
          item_replicas = Scala::Collection::JavaConversions.as_java_iterable(item.last).to_a
          diff_replicas = replicas - item_replicas
          unless diff_replicas.empty?
            transition_replicas = replicas + diff_replicas
            return Scala::Collection::JavaConversions.as_scala_iterable(transition_replicas)
          end
        end
      end

      false
    end

    def manage_progress_state(reassignment)
      delete_previous_state
      json = reassignment_json(reassignment)
      @zk_client.create_znode(@state_path, json)
    end

    def delete_previous_state
      if @zk_client.exists?(@state_path)
        @zk_client.delete_znode(@state_path)
      end
    end

    def delete_previous_overflow
      overflow = @zk_client.get_children(@overflow_path)
      overflow.foreach do |index|
        @zk_client.delete_znode(overflow_path(index))
      end
    rescue ZkClient::Exception::ZkNoNodeException
      # no-op
    rescue => e
      puts e.backtrace.join($/)
    end

    def manage_overflow(reassignments)
      delete_previous_overflow
      empty_map = Scala::Collection::Map.empty
      overflow = reassignments.reduce(empty_map) do |acc, data|
        acc.send('++', data)
      end
      if overflow.size > 0
        write_overflow(split(overflow))
      end
    end

    def overflow_path(index)
      [@overflow_path, index].join('/')
    end

    def write_overflow(reassignments)
      reassignments.each_with_index do |reassignment, index|
        overflow_json = reassignment_json(reassignment)
        @zk_client.create_znode(overflow_path(index), overflow_json)
      end
    end

    def reassignment_json(reassignment)
      zk_utils.format_as_reassignment_json(reassignment)
    end

    def parse_reassignment_json(json)
      zk_utils.parse_partition_reassignment_data(json)
    end

    def zk_utils
      @zk_utils ||= Kafka::Utils::ZkUtils.new(nil, nil, false)
    end

    def maybe_split_by_limit(reassignment, limit=nil)
      if limit
        splitted = ScalaEnumerable.new(reassignment.grouped(limit)).map(&:seq)
      else
        splitted = [reassignment]
      end
    end

    def split(reassignment, limit=nil)
      splitted = maybe_split_by_limit(reassignment, limit)
      bytesize = reassignment_json(splitted.first).bytesize
      while bytesize > JSON_MAX_SIZE do
        splitted = splitted.flat_map do |s|
          group_size = s.size.fdiv(2).round
          ScalaEnumerable.new(s.grouped(group_size)).map(&:seq)
        end
        bytesize = reassignment_json(splitted.first).bytesize
      end
      splitted
    end
  end
end
