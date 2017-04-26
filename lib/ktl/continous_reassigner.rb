# encoding: utf-8

module Ktl
  class ContinousReassigner < Reassigner
    include ZkClient::IZkDataListener

    def initialize(zk_client, options={})
      super(zk_client, options)
      @latch = JavaConcurrent::CountDownLatch.new(1)
      @sleeper = options[:sleeper] || java.lang.Thread
      @delay = options[:delay] || 5
    end

    def execute(reassignment)
      Signal.trap('SIGINT', proc { puts 'Exiting due to Ctrl-C'; @latch.count_down })
      @zk_client.watch_data(zk_utils.class.reassign_partitions_path, self)
      if reassignment_in_progress?
        @logger.info 'reassignment already in progress, watching for changes...'
      else
        reassign(reassignment)
      end
      @latch.await
    end

    def handle_data_change(path, data)
      parsed_data = JSON.parse(data)
      if (partitions = parsed_data['partitions'])
        partitions = partitions.map { |r| r.values_at('topic', 'partition').join(':') }
        puts sprintf('%d partitions left to reassign (%p)', partitions.size, partitions)
      else
        puts sprintf('Data without `partitions` key: %p', parsed_data)
      end
    rescue => e
      puts sprintf('Bad data: %p', data)
    end

    def handle_data_deleted(path)
      reassignment = load_overflow
      if reassignment.empty?
        @zk_client.unsubscribe_data(zk_utils.class.reassign_partitions_path, self)
        delete_previous_state
        @latch.count_down
      else
        puts sprintf('Waiting %ds before next assignment', @delay)
        @sleeper.sleep(@delay * 1000)
        reassign(reassignment)
      end
    end

    private

    def reassign(reassignment)
      reassignments = split(reassignment, @limit)
      actual_reassignment = reassignments.shift
      json = reassignment_json(actual_reassignment)
      @zk_client.reassign_partitions(json)
      manage_overflow(reassignments)
      manage_progress_state(actual_reassignment)
    end
  end
end
