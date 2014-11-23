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
      overflow
    end

    def execute(reassignment)
      json = reassignment_json(reassignment)
      reassignments = split(reassignment, json.bytesize)
      json = reassignment_json(reassignments.next)
      @zk_client.reassign_partitions(json)
      manage_overflow(reassignments)
    end

    private

    JSON_MAX_SIZE = 1024**3

    def manage_overflow(reassignments)
      if reassignments.has_next?
        write_overflow(reassignments)
      else
        delete_old_overflow
      end
    end

    def overflow_base_path
      @overflow_base_path ||= %(/ktl/overflow/#{@type})
    end

    def overflow_path(index)
      %(#{overflow_base_path}/#{index})
    end

    def write_overflow(reassignments)
      index = 0
      while reassignments.has_next?
        overflow_json = reassignment_json(reassignments.next)
        @zk_client.create_znode(overflow_path(index), overflow_json)
        index += 1
      end
    end

    def delete_old_overflow
      @zk_client.delete_znode(overflow_base_path, recursive: true)
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
      reassignment.grouped(group_size)
    end
  end
end
