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
      reassignments = split(reassignment, @limit)
      reassignment_candidates = reassignments.shift
      actual_reassignment = Scala::Collection::Map.empty
      next_step_assignments = Scala::Collection::Map.empty
      ScalaEnumerable.new(reassignment_candidates).each do |pr|
        topic_and_partition, replicas = pr.elements
        if step1_replicas = is_two_step_operation(topic_and_partition, replicas)
          next_step_assignments += pr
          actual_reassignment += Scala::Tuple.new(topic_partition, step1_replicas)
        else
          actual_reassignment += pr
        end
        brokers = Scala::Collection::JavaConversions.as_java_iterable(replicas).to_a
        @logger.info "Assigning #{topic_and_partition.topic},#{topic_and_partition.partition} to #{brokers.join(',')}" if @log_assignments
      end
      json = reassignment_json(actual_reassignment)
      @zk_client.reassign_partitions(json)
      manage_overflow(next_step_assignments.send('++', reassignments))
      manage_progress_state(actual_reassignment)
    end

    private

    JSON_MAX_SIZE = 1024**2

    def is_two_step_operation(topic_and_partition, replicas)
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
