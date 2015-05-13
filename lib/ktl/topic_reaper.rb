# encoding: utf-8

module Ktl
  class TopicReaper
    def initialize(kafka_client, zk_client, filter, options={})
      @kafka_client = kafka_client
      @zk_client = zk_client
      @filter = filter
      @parallel = options[:parallel] || 10
      @delay = options[:delay] || 10
      @logger = options[:logger] ||  NullLogger.new
      @utils = options[:utils] || Kafka::Utils
      @sleeper = options[:sleeper] || Java::JavaLang::Thread
    end

    def execute
      partitions_by_topic = @kafka_client.partitions(@filter)
      earliest = @kafka_client.earliest_offset(partitions_by_topic)
      latest = @kafka_client.latest_offset(partitions_by_topic)
      empty = filter_empty_topics(partitions_by_topic, earliest, latest)
      @logger.info %(marking #{empty.size} topics for deletion)
      empty.each_slice(@parallel) do |topics|
        delete_topics(topics)
      end
    end

    private

    def filter_empty_topics(partitions_by_topic, earliest, latest)
      empty = partitions_by_topic.select do |topic, partitions|
        partitions.all? do |partition|
          earliest[topic][partition] == latest[topic][partition]
        end
      end
      empty.keys
    end

    def delete_topics(topics)
      @logger.debug { 'marking %d topics (%s) for deletion' % [topics.size, topics.join(', ')] }
      topics.each do |topic|
        @utils.delete_topic(@zk_client.raw_client, topic)
      end
      if @delay > 0
        @logger.debug { 'waiting %ds before next batch' % [@delay] }
        @sleeper.sleep(@delay * 1000)
      end
    end
  end
end
