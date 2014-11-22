# encoding: utf-8

module Ktl
  class Topic < Command
    desc 'list', 'list current topics'
    def list
      with_zk_client do |zk_client|
        topic_options = Kafka::Admin.to_topic_options(options.merge(list: nil))
        Kafka::Admin::TopicCommand.list_topics(zk_client.raw_client, topic_options)
      end
    end

    desc 'create', 'create one or more new topics'
    option :partitions, aliases: %w[-p], default: '1', desc: 'partitions for new topic(s)'
    option :replication_factor, aliases: %w[-r], default: '1', desc: 'replication factor for new topic(s)'
    option :replica_assignment, aliases: %w[-a], desc: 'manual replica assignment'
    def create(*names)
      with_zk_client do |zk_client|
        names.each do |name|
          opts = options.merge(create: nil, topic: name)
          topic_options = Kafka::Admin.to_topic_options(opts)
          Kafka::Admin::TopicCommand.create_topic(zk_client.raw_client, topic_options)
        end
      end
    end

    desc 'add-partitions', 'add partitions to one or more existing topics'
    option :partitions, aliases: %w[-p], required: true, desc: 'new number of partitions'
    def add_partitions(*names)
      with_zk_client do |zk_client|
        names.each do |name|
          opts = options.merge(alter: nil, topic: name)
          topic_options = Kafka::Admin.to_topic_options(opts)
          Kafka::Admin::TopicCommand.alter_topic(zk_client.raw_client, topic_options)
        end
      end
    end

    desc 'delete', 'delete topics matching given regexp'
    def delete(regexp)
      with_zk_client do |zk_client|
        topics = zk_client.all_topics
        topics = topics.filter { |t| !!t.match(regexp) }
        say 'about to delete %d topics' % topics.size
        topics.foreach do |topic|
          Kafka::Admin::AdminUtils.delete_topic(zk_client.raw_client, topic)
        end
      end
    end
  end
end
