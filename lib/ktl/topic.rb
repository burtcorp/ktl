# encoding: utf-8

module Ktl
  class Topic < Command
    desc 'list', 'List current topics'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    def list
      with_zk_client do |zk_client|
        topic_options = Kafka::Admin.to_topic_options(options.merge(list: nil))
        Kafka::Admin::TopicCommand.list_topics(zk_client.raw_client, topic_options)
      end
    end

    desc 'describe', 'Describe (optionally filtered) topics'
    option :unavailable, aliases: '-u', desc: 'Describe unavailable partitions for topic(s)'
    option :with_overrides, aliases: '-w', desc: 'Describe topics with config. overrides'
    option :under_replicated, aliases: '-r', desc: 'Describe under-replicated partitions for topic(s)'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    def describe(regexp=nil)
      with_zk_client do |zk_client|
        opts = {describe: nil}
        opts[:topic] = regexp if regexp
        opts[:topics_with_overrides] = nil if options.with_overrides?
        opts[:unavailable_partitions] = nil if options.unavailable?
        opts[:under_replicated_partitions] = nil if options.under_replicated?
        topic_options = Kafka::Admin.to_topic_options(opts)
        Kafka::Admin::TopicCommand.describe_topic(zk_client.raw_client, topic_options)
      end
    end

    desc 'create NAMES..', 'Create one or more new topics'
    option :partitions, aliases: %w[-p], default: '1', desc: 'Partitions for new topic(s)'
    option :replication_factor, aliases: %w[-r], default: '1', desc: 'Replication factor for new topic(s)'
    option :replica_assignment, aliases: %w[-a], desc: 'Manual replica assignment'
    option :config, aliases: %w[-c], desc: 'Key-value pairs of configuration options', type: :hash, default: {}
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    def create(*names)
      with_zk_client do |zk_client|
        names.each do |name|
          opts = options.merge(create: nil, topic: name)
          topic_options = Kafka::Admin.to_topic_options(opts)
          silence_scala do
            Kafka::Admin::TopicCommand.create_topic(zk_client.raw_client, topic_options)
          end
          message = %(created topic "#{name}" with #{options.partitions} partition(s))
          message << %(, and replication factor #{options.replication_factor})
          message << %(, with replica assignment: #{options.replica_assignment}) if options.replica_assignment
          message << %(, with config: #{options.config}) unless options.config.empty?
          logger.info(message)
        end
      end
    end

    desc 'add-partitions NAMES..', 'Add partitions to one or more existing topics'
    option :partitions, aliases: %w[-p], required: true, desc: 'New number of partitions'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    def add_partitions(*names)
      with_zk_client do |zk_client|
        names.each do |name|
          opts = options.merge(alter: nil, topic: name)
          topic_options = Kafka::Admin.to_topic_options(opts)
          logger.warn %(if "#{name}" uses keyed messages, the partition logic or ordering of the messages will be affected)
          silence_scala do
            Kafka::Admin::TopicCommand.alter_topic(zk_client.raw_client, topic_options)
          end
          logger.info %(increased partitions to #{options.partitions} for "#{name}")
        end
      end
    end

    desc 'delete REGEXP', 'Delete topics matching given regexp'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    def delete(regexp)
      with_zk_client do |zk_client|
        topics = zk_client.all_topics
        topics = topics.filter { |t| !!t.match(regexp) }
        logger.info %(about to mark #{topics.size} topics for deletion)
        topics.foreach do |topic|
          Kafka::Utils.delete_topic(zk_client.raw_client, topic)
          logger.debug %(successfully marked "#{topic}" for deletion)
        end
      end
    end

    desc 'reaper [REGEXP]', 'Delete empty topics (optionally matching regexp)'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    option :parallel, aliases: %w[-p], desc: 'Number of topics to delete in parallel', type: :numeric, default: 10
    option :delay, aliases: %w[-d], desc: 'Delay between deletes', type: :numeric
    def reaper(regexp='.*')
      with_kafka_client do |kafka_client|
        with_zk_client do |zk_client|
          reaper_options = options.merge(logger: logger)
          regexp = Regexp.new(regexp)
          reaper = TopicReaper.new(kafka_client, zk_client, regexp, reaper_options)
          reaper.execute
        end
      end
    end

    desc 'alter REGEXP', 'Alter topic configuration'
    option :add, aliases: %w[-a], desc: 'Key-value pairs of config options to add', type: :hash, default: {}
    option :remove, aliases: %w[-r], desc: 'Key-value pairs of config options to remove', type: :array, default: []
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    def alter(regexp)
      with_zk_client do |zk_client|
        opts = {zookeeper: options.zookeeper, topic: regexp}
        opts[:config] = options.add.dup unless options.add.empty?
        opts[:delete_config] = options.remove.dup unless options.remove.empty?
        if opts[:config] || opts[:delete_config]
          topic_options = Kafka::Admin.to_topic_options(opts)
          silence_scala do
            Kafka::Admin::TopicCommand.alter_topic(zk_client.raw_client, topic_options)
          end
          logger.info %(updated configuration for topics matching "#{regexp}")
        else
          raise ArgumentError, 'missing --add or --remove option'
        end
      end
    end
  end
end
