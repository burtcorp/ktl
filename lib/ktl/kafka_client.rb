# encoding: utf-8

module Ktl
  class KafkaClient
    def self.create(config)
      new(config).setup
    end

    def initialize(config)
      @hosts = config[:hosts]
      @backoff = (config[:backoff] || 1).to_i * 1000
      @max_offsets = config[:max_offsets] || 10
      @logger = config[:logger] || NullLogger.new
      @consumer_impl = config[:consumer_impl] || Heller::Consumer
      @sleeper = config[:sleeper] || Java::JavaLang::Thread
      @consumers = JavaConcurrent::ConcurrentHashMap.new
      @mutex = Mutex.new
    end

    def setup
      @logger.info(sprintf('Connecting to Kafka at %s', @hosts.join(',')))
      @hosts.each do |connection_string|
        @consumers[connection_string] = @consumer_impl.new(connection_string)
      end
      self
    end

    def partitions(filter=/.*/)
      result = Hash.new { |h, k| h[k] = [] }
      metadata = fetch_metadata
      metadata.each do |topic, partition_metadata|
        result[topic] << partition_metadata.partition_id if topic =~ filter
      end
      result
    end

    def earliest_offset(topic_partitions)
      fetch_offsets(topic_partitions, Heller::OffsetRequest.earliest_time) { |offsets| offsets.min }
    end

    def latest_offset(topic_partitions)
      fetch_offsets(topic_partitions, Heller::OffsetRequest.latest_time) { |offsets| offsets.max }
    end

    def offset_before(topic_partitions, time)
      fetch_offsets(topic_partitions, time.to_i * 1000) { |offsets| offsets.max }
    end

    def close
      @consumers.each_value(&:close)
    end

    private

    def fetch_metadata
      consumer = random_consumer
      consumer.metadata
    end

    def random_consumer
      @consumers.values.sample
    end

    def consumer_for(connection_string)
      if (consumer = @consumers[connection_string])
        consumer
      else
        consumer = @consumer_impl.new(connection_string)
        @consumers[connection_string] = consumer
        consumer
      end
    end

    def build_offset_requests(topic_partitions, timestamp)
      requests = Hash.new { |h, k| h[k] = [] }
      metadata = fetch_metadata
      topic_partitions.each do |topic, partitions|
        partitions.each do |partition|
          broker = metadata.leader_for(topic, partition)
          consumer = consumer_for(broker.connection_string)
          request = Heller::OffsetRequest.new(topic, partition, timestamp, @max_offsets)
          requests[consumer] << request
        end
      end
      requests
    end

    def fetch_offsets(topic_partitions, timestamp, &extracter)
      result = Hash.new { |h, k| h[k] = {} }
      consumer_requests = build_offset_requests(topic_partitions, timestamp)
      until consumer_requests.empty? do
        failed = Hash.new { |h, k| h[k] = [] }
        consumer_requests.each do |consumer, requests|
          response = consumer.offsets_before(requests)
          requests.each do |request|
            topic, partition = request.topic, request.partition
            if response.error(topic, partition) > 0
              failed[topic] << partition
            else
              offsets = response.offsets(topic, partition)
              offsets = extracter.call(offsets) if block_given?
              result[topic][partition] = offsets
            end
          end
        end
        break if failed.empty?
        @logger.debug do
          topics = failed.keys.size
          partitions = failed.values.map(&:size).reduce(:+)
          sprintf('Failed to find metadata for %d topics (%d partitions), retrying...', topics, partitions)
        end
        @sleeper.sleep(@backoff)
        consumer_requests = build_offset_requests(failed, timestamp)
      end
      result
    end
  end
end
