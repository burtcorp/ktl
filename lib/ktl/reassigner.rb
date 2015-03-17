# encoding: utf-8

module Ktl
  class Reassigner
    def initialize(type, zk_client, options={})
      @type = type
      @zk_client = zk_client
      @json_max_size = options[:json_max_size] || JSON_MAX_SIZE
    end

    def reassignment_in_progress?
      partitions = @zk_client.partitions_being_reassigned
      partitions.size > 0
    end

    def overflow?
      overflow_znodes = @zk_client.get_children(overflow_base_path)
      overflow_znodes.size > 0
    rescue ZkClient::Exception::ZkNoNodeException
      false
    end

    def load_overflow
      overflow = Scala::Collection::Map.empty
      overflow_nodes = @zk_client.get_children(overflow_base_path)
      overflow_nodes.foreach do |index|
        overflow_json = @zk_client.read_data(overflow_path(index)).first
        data = parse_reassignment_json(overflow_json)
        overflow = overflow.send('++', data)
      end
      delete_previous_overflow
      overflow
    end

    def execute(reassignment)
      json = reassignment_json(reassignment)
      reassignments = split(reassignment, json.bytesize)
      actual_reassignment = reassignments.shift
      json = reassignment_json(actual_reassignment)
      @zk_client.reassign_partitions(json)
      manage_overflow(reassignments)
      manage_progress_state(reassignments.unshift(actual_reassignment))
    end

    private

    JSON_MAX_SIZE = 1000**2

    def manage_progress_state(reassignments)
      delete_previous_state
      reassignments.each_with_index do |reassignment, index|
        json = reassignment_json(reassignment)
        @zk_client.create_znode(state_path(index), json)
      end
    end

    def delete_previous_state
      state_path = %(/ktl/reassign/#{@type})
      if @zk_client.exists?(state_path)
        @zk_client.delete_znode(state_path, recursive: true)
      end
    end

    def delete_previous_overflow
      if @zk_client.exists?(overflow_base_path)
        @zk_client.delete_znode(overflow_base_path, recursive: true)
      end
    end

    def state_path(index)
      %(/ktl/reassign/#{@type}/#{index})
    end

    def manage_overflow(reassignments)
      delete_previous_overflow
      write_overflow(reassignments)
    end

    def overflow_base_path
      @overflow_base_path ||= %(/ktl/overflow/#{@type})
    end

    def overflow_path(index)
      %(#{overflow_base_path}/#{index})
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

    def split(reassignment, bytesize)
      groups = [bytesize.fdiv(@json_max_size).round, 1].max
      group_size = reassignment.size.fdiv(groups).round
      ScalaEnumerable.new(reassignment.grouped(group_size)).map(&:seq)
    end
  end
end
