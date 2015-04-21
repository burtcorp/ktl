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
        shell.say 'remaining partitions to reassign: %d (%.2f%% done)' % [remaining_size, done_percentage]
        if @options[:verbose]
          shell.print_table(table_data(in_progress), indent: 2)
        end
      else
        shell.say 'no partitions remaining to reassign'
      end
    end

    private

    def state_path
      @state_path ||= '/ktl/reassign/%s' % @command.to_s
    end

    def table_data(reassignments)
      topics = reassignments.group_by { |r| r['topic'] }
      table = topics.map do |t, r|
        reassignments = r.sort_by { |r| r['partition'] }
        reassignments = reassignments.map { |r| '%d => %s' % [r['partition'], r['replicas'].inspect] }.join(', ')
        [t, reassignments]
      end.sort_by(&:first)
      table.unshift(%w[topic assignments])
      table
    end

    def reassignment_in_progress
      read_json(@utils.reassign_partitions_path).fetch('partitions')
    rescue ZkClient::Exception::ZkNoNodeException
      {}
    end

    def original_reassignment
      read_json(state_path).fetch('partitions')
    rescue ZkClient::Exception::ZkNoNodeException
      {}
    end

    def read_json(path)
      JSON.parse(@zk_client.read_data(path).first)
    end
  end
end
