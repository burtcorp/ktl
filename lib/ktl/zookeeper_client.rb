# encoding: utf-8

module Ktl
  class ZookeeperClient
    attr_reader :utils

    def initialize(uri, options={})
      @uri = uri
      @threadpool = options[:threadpool] || JavaConcurrent::Executors.new_fixed_thread_pool(CONCURRENCY)
      @utils = options[:utils] || Kafka::Utils::ZkUtils.apply(@uri, 5000, 5000, false)
    end

    def setup
      @submit = @threadpool.java_method(:submit, [java.lang.Class.for_name('java.util.concurrent.Callable')])
      self
    end

    def close
      @threadpool.shutdown_now if @threadpool
      @utils.close
    end

    def raw_client
      @utils
    end

    def all_partitions
      @utils.get_all_partitions
    end

    def all_topics
      @utils.get_all_topics
    end

    def brokers
      @utils.get_all_brokers_in_cluster
    end

    def broker_ids
      @utils.get_sorted_broker_list
    end

    def leader_and_isr_for(partitions)
      @utils.get_partition_leader_and_isr_for_topics(@utils.class.create_zk_client(@uri, 5_000, 5_000), partitions)
    end

    def partitions_for_topics(topics)
      request(:get_partitions_for_topics, topics)
    end

    def replica_assignment_for_topics(topics)
      request(:get_replica_assignment_for_topics, topics)
    end

    def partitions_being_reassigned
      @utils.get_partitions_being_reassigned
    end

    def reassign_partitions(json)
      @utils.create_persistent_path(@utils.class.reassign_partitions_path, json, no_acl)
    end

    def create_znode(path, data='')
      @utils.create_persistent_path(path, data, no_acl)
    end

    def delete_znode(path, options={})
      if options[:recursive]
        @utils.delete_path_recursive(path)
      else
        @utils.delete_path(path)
      end
    end

    def read_data(path)
      @utils.read_data(path)
    end

    def get_children(path)
      @utils.get_children(path)
    end

    def exists?(path)
      @utils.path_exists(path)
    end

    def watch_state(path, listener)
      @client.subscribe_state_changes(path, listener)
    end

    def watch_data(path, listener)
      @client.subscribe_data_changes(path, listener)
    end

    def watch_child(path, listener)
      @client.subscribe_child_changes(path, listener)
    end

    def unsubscribe_data(path, listener)
      @client.unsubscribe_data_changes(path, listener)
    end

    private

    CONCURRENCY = 8

    def no_acl
      Kafka::Utils::ZkUtils::DefaultAcls(false)
    end

    def request(method, input)
      chunk_size = [(input.size.to_f / CONCURRENCY).round, 1].max
      groups = ScalaEnumerable.new(input.grouped(chunk_size).to_seq)
      futures = groups.map do |slice|
        @submit.call { @utils.send(method, slice) }
      end
      merge(futures.map(&:get))
    end

    def merge(results)
      result = Scala::Collection::Map.empty
      results.reduce(result) do |acc, v|
        acc.send('++', v)
      end
    end
  end
end
