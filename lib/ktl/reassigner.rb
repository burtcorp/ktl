# encoding: utf-8

module Ktl
  class Reassigner
    def initialize(zk_client, options={})
      @zk_client = zk_client
      @limit = options[:limit]
      @overflow_path = '/ktl/overflow'
      @state_path = '/ktl/reassign'
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
      delete_previous_overflow
      overflow
    end

    def execute(reassignment)
      reassignments = split(reassignment, @limit)
      actual_reassignment = reassignments.shift
      json = reassignment_json(actual_reassignment)
      @zk_client.reassign_partitions(json)
      manage_overflow(reassignments)
      manage_progress_state(actual_reassignment)
    end

    private

    JSON_MAX_SIZE = 1024**2

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
      Kafka::Utils::ZkUtils.get_partition_reassignment_zk_data(reassignment)
    end

    def parse_reassignment_json(json)
      Kafka::Utils::ZkUtils.parse_partition_reassignment_data(json)
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
