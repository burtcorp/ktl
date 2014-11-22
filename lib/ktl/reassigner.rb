# encoding: utf-8

module Ktl
  class Reassigner
    attr_reader :partitions

    def initialize(type, zk_client, options={})
      @type = type
      @zk_client = zk_client
      @json_max_size = options[:json_max_size] || JSON_MAX_SIZE
    end

    def in_progress?
      @partitions ||= @zk_client.partitions_being_reassigned
      @partitions.size > 0
    end

    def overflow?
      File.exists?(overflow_filename)
    end

    def load
      overflow_json = File.read(overflow_filename)
      parse_reassignment_json(overflow_json)
    end

    def execute(reassignment)
      json = reassignment_json(reassignment)
      if json.bytesize >= @json_max_size
        reassignments = split(reassignment, json.bytesize)
        json = reassignment_json(reassignments.next)
        write_overflow(reassignments)
      end
      @zk_client.reassign_partitions(json)
    end

    private

    JSON_MAX_SIZE = 1024**3

    def overflow_filename
      @overflow_filename ||= %(.#{@type}-overflow.json)
    end

    def write_overflow(reassignments)
      File.open(overflow_filename, 'w+') do |file|
        remaining = reassignments.reduce_left { |e, r| r.send('++', e) }
        file.puts(reassignment_json(remaining))
      end
    end

    def reassignment_json(reassignment)
      Kafka::Utils::ZkUtils.get_partition_reassignment_zk_data(reassignment)
    end

    def parse_reassignment_json(json)
      Kafka::Utils::ZkUtils.parse_partition_reassignment_data(json)
    end

    def split(reassignment, bytesize)
      groups = bytesize.fdiv(@json_max_size).round
      group_size = reassignment.size.fdiv(groups).round
      reassignment.grouped(group_size)
    end
  end
end
