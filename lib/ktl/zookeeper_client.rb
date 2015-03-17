# encoding: utf-8

module Ktl
  class ZookeeperClient
    def initialize(uri, options={})
      @uri = uri
      @threadpool = options[:threadpool] || JavaConcurrent::Executors.new_fixed_thread_pool(CONCURRENCY)
      @utils = options[:utils] || Kafka::Utils::ZkUtils
    end

    def setup
      @client = Kafka::Utils.new_zk_client(@uri)
      @submit = @threadpool.java_method(:submit, [java.lang.Class.for_name('java.util.concurrent.Callable')])
      self
    end

    def close
      @threadpool.shutdown_now if @threadpool
      @client.close if @client
    end

    def raw_client
      @client
    end

    def all_partitions
      @utils.get_all_partitions(@client)
    end

    def all_topics
      @utils.get_all_topics(@client)
    end

    def brokers
      @utils.get_all_brokers_in_cluster(@client)
    end

    def broker_ids
      @utils.get_sorted_broker_list(@client)
    end

    def leader_and_isr_for(partitions)
      request(:get_partition_leader_and_isr_for_topics, partitions)
    end

    def partitions_for_topics(topics)
      request(:get_partitions_for_topics, topics)
    end

    def replica_assignment_for_topics(topics)
      request(:get_replica_assignment_for_topics, topics)
    end

    def partitions_being_reassigned
      @utils.get_partitions_being_reassigned(@client)
    end

    def reassign_partitions(json)
      @utils.create_persistent_path(@client, @utils.reassign_partitions_path, json)
    end

    def create_znode(path, data='')
      @utils.create_persistent_path(@client, path, data)
    end

    def delete_znode(path, options={})
      if options[:recursive]
        @utils.delete_path_recursive(@client, path)
      else
        @utils.delete_path(@client, path)
      end
    end

    def read_data(path)
      @utils.read_data(@client, path)
    end

    def get_children(path)
      @utils.get_children(@client, path)
    end

    def exists?(path)
      @utils.path_exists(@client, path)
    end

    private

    CONCURRENCY = 8

    def request(method, input)
      chunk_size = [(input.size.to_f / CONCURRENCY).round, 1].max
      groups = ScalaEnumerable.new(input.grouped(chunk_size).to_seq)
      futures = groups.map do |slice|
        @submit.call { @utils.send(method, @client, slice) }
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
