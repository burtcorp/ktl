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
      remaining = diff(current_reassignment, remaining_reassignment)
      if (partitions = remaining['partitions'])
        shell.say 'remaining partitions to reassign: %d' % partitions.size
        if @options[:verbose]
          shell.print_table(table_data(remaining), indent: 2)
        end
      else
        shell.say 'no partitions remaining to reassign'
      end
      queued = find_queued_reassignments
      if queued.any?
        shell.say 'there are %d queued reassignments' % queued.size
        if @options[:verbose]
          queued = queued.reduce({'partitions' => []}) do |acc, r|
            acc['partitions'] += r['partitions']
            acc
          end
          shell.print_table(table_data(queued), indent: 2)
        end
      end
    end

    private

    def find_queued_reassignments
      path = %(/ktl/reassign/#{@command})
      znodes = ScalaEnumerable.new(@zk_client.get_children(path)).sort
      znodes.shift
      znodes.map { |z| read_json(%(#{path}/#{z})) }
    rescue ZkClient::Exception::ZkNoNodeException
      []
    end

    def table_data(reassignments)
      table = reassignments['partitions'].map do |r|
        r.values_at(*%w[topic partition replicas])
      end.sort_by { |r| [r[0], r[1]] }
      table.unshift(%w[topic partition replicas])
      table
    end

    def current_reassignment
      read_json(@utils.reassign_partitions_path)
    rescue ZkClient::Exception::ZkNoNodeException
      {}
    end

    def remaining_reassignment
      path = %(/ktl/reassign/#{@command})
      znode = ScalaEnumerable.new(@zk_client.get_children(path)).sort.first
      read_json(%(#{path}/#{znode}))
    rescue ZkClient::Exception::ZkNoNodeException
      puts 'failed to read json from %s' % %(#{path}/#{znode})
      {}
    end

    def read_json(path)
      JSON.parse(@zk_client.read_data(path).first)
    end

    def diff(current, executing)
      if current['partitions']
        if current['partitions'] == executing['partitions']
          current
        else
          remaining = current.dup
          remaining['partitions'] = executing['partitions'] - current['partitions']
          remaining
        end
      else
        current
      end
    end
  end
end

