# encoding: utf-8

module Ktl
  class Topic < Command
    desc 'list', 'list current topics'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    def list
      with_zk_client do |zk_client|
        topic_options = Kafka::Admin.to_topic_options(options.merge(list: nil))
        Kafka::Admin::TopicCommand.list_topics(zk_client.raw_client, topic_options)
      end
    end

    desc 'describe', 'describe (optionally filtered) topics'
    option :unavailable, aliases: '-u', desc: 'describe unavailable partitions for topic(s)'
    option :with_overrides, aliases: '-w', desc: 'describe topics with config. overrides'
    option :under_replicated, aliases: '-r', desc: 'describe under-replicated partitions for topic(s)'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    def describe(regexp=nil)
      with_zk_client do |zk_client|
        opts = {describe: nil}
        opts[:topic] = regexp if regexp
        opts[:topics_with_overrides] = nil if options.with_overrides
        opts[:unavailable_partitions] = nil if options.unavailable
        opts[:under_replicated_partitions] = nil if options.under_replicated
        topic_options = Kafka::Admin.to_topic_options(opts)
        Kafka::Admin::TopicCommand.describe_topic(zk_client.raw_client, topic_options)
      end
    end

    desc 'create NAMES..', 'create one or more new topics'
    option :partitions, aliases: %w[-p], default: '1', desc: 'partitions for new topic(s)'
    option :replication_factor, aliases: %w[-r], default: '1', desc: 'replication factor for new topic(s)'
    option :replica_assignment, aliases: %w[-a], desc: 'manual replica assignment'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    def create(*names)
      with_zk_client do |zk_client|
        names.each do |name|
          opts = options.merge(create: nil, topic: name)
          topic_options = Kafka::Admin.to_topic_options(opts)
          Kafka::Admin::TopicCommand.create_topic(zk_client.raw_client, topic_options)
        end
      end
    end

    desc 'add-partitions NAMES..', 'add partitions to one or more existing topics'
    option :partitions, aliases: %w[-p], required: true, desc: 'new number of partitions'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    def add_partitions(*names)
      with_zk_client do |zk_client|
        names.each do |name|
          opts = options.merge(alter: nil, topic: name)
          topic_options = Kafka::Admin.to_topic_options(opts)
          Kafka::Admin::TopicCommand.alter_topic(zk_client.raw_client, topic_options)
        end
      end
    end

    desc 'delete REGEXP', 'delete topics matching given regexp'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    def delete(regexp)
      with_zk_client do |zk_client|
        topics = zk_client.all_topics
        topics = topics.filter { |t| !!t.match(regexp) }
        say 'about to delete %d topics' % topics.size
        topics.foreach do |topic|
          Kafka::Utils.delete_topic(zk_client.raw_client, topic)
        end
      end
    end

    desc 'reaper [REGEXP]', 'delete empty topics (optionally matching regexp)'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    option :parallel, aliases: %w[-p], desc: 'number of topics to delete in parallel', type: :numeric, default: 10
    option :delay, aliases: %w[-d], desc: 'delay between deletes', type: :numeric
    def reaper(regexp='.*')
      with_kafka_client do |kafka_client|
        with_zk_client do |zk_client|
          reaper = TopicReaper.new(kafka_client, zk_client, Regexp.new(regexp), options)
          reaper.execute
        end
      end
    end
  end
end
