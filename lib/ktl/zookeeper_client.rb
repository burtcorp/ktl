# encoding: utf-8

module Ktl
  class ZookeeperClient
    def initialize(uri, options={})
      @uri = uri
      @threadpool = options[:threadpool] || JavaConcurrent::Executors.new_fixed_thread_pool(4)
      @zk_utils = options[:zk_utils] || Kafka::Utils::ZkUtils
    end

    def setup
      @client = Kafka::Utils.new_zk_client(@uri)
      @submit = @threadpool.java_method(:submit, [Java::JavaLang::Class.for_name('java.util.concurrent.Callable')])
      self
    end

    def close
      @threadpool.shutdown_now if @threadpool
      @client.close if @client
    end

    def all_partitions
      @zk_utils.get_all_partitions(@client)
    end

    def brokers
      @zk_utils.get_all_brokers_in_cluster(@client)
    end

    def leader_and_isr_for(partitions)
      iterator = partitions.grouped(chunk_size(partitions))
      futures = []
      while iterator.has_next? do
        slice = iterator.next
        futures << @submit.call do
          @zk_utils.get_partition_leader_and_isr_for_topics(@client, slice)
        end
      end
      result = Scala::Collection::Mutable::HashMap.new
      results = futures.map(&:get)
      results.each do |v|
        result = result.send('++', v)
      end
      result
    end

    private

    def chunk_size(list)
      (list.size / 4.0).round
    end
  end
end
