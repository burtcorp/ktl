# encoding: utf-8

module Ktl
  class ReassignmentProgress
    def initialize(zk_client, command, options={})
      @zk_client = zk_client
      @command = command
      @utils = options[:utils] || Kafka::Utils::ZkUtils
      @options = options
    end

    def display(shell)
      in_progress = reassignment_in_progress
      original = original_reassignment
      if in_progress && !in_progress.empty?
        original_size, remaining_size = original.size, in_progress.size
        done_percentage = (original_size - remaining_size).fdiv(original_size) * 100
        shell.say 'remaining partitions to reassign: %d (%.f%% done)' % [remaining_size, done_percentage]
        if @options[:verbose]
          shell.print_table(table_data(in_progress), indent: 2)
        end
      else
        shell.say 'no partitions remaining to reassign'
      end
      queued = find_queued_reassignments
      if queued.any?
        shell.say 'there are %d queued reassignments' % queued.size
        if @options[:verbose]
          queued = queued.flat_map { |r| r['partitions'] }
          shell.print_table(table_data(queued), indent: 2)
        end
      end
    end

    private

    def state_path
      @state_path ||= '/ktl/reassign/%s' % @command.to_s
    end

    def state_znodes
      ScalaEnumerable.new(@zk_client.get_children(state_path)).sort
    end

    def find_queued_reassignments
      znodes = state_znodes
      znodes.shift
      znodes.map { |z| read_json(%(#{state_path}/#{z})) }
    rescue ZkClient::Exception::ZkNoNodeException
      []
    end

    def table_data(reassignments)
      table = reassignments.map do |r|
        r.values_at(*%w[topic partition replicas])
      end.sort_by { |r| [r[0], r[1]] }
      table.unshift(%w[topic partition replicas])
      table
    end

    def reassignment_in_progress
      read_json(@utils.reassign_partitions_path).fetch('partitions')
    rescue ZkClient::Exception::ZkNoNodeException
      {}
    end

    def original_reassignment
      znode = state_znodes.first
      read_json(%(#{state_path}/#{znode})).fetch('partitions')
    rescue ZkClient::Exception::ZkNoNodeException
      {}
    end

    def read_json(path)
      JSON.parse(@zk_client.read_data(path).first)
    end
  end
end

