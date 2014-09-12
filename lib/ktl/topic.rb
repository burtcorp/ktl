# encoding: utf-8

module Ktl
  class Topic < Thor
    class_option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri w/ optional chroot'

    desc 'list', 'list current topics'
    def list
      topic_options = Kafka::Admin.to_topic_options(options.merge(list: nil))
      Kafka::Admin::TopicCommand.list_topics(zk_client, topic_options)
      zk_client.close
    end

    desc 'create', 'create one or more new topics'
    option :partitions, aliases: %w[-p], default: '1', desc: 'partitions for new topic(s)'
    option :replication_factor, aliases: %w[-r], default: '1', desc: 'replication factor for new topic(s)'
    def create(*names)
      names.each do |name|
        opts = options.merge(create: nil, topic: name)
        topic_options = Kafka::Admin.to_topic_options(opts)
        Kafka::Admin::TopicCommand.create_topic(zk_client, topic_options)
      end
      zk_client.close
    end

    desc 'expand', 'add partitions to one or more existing topics'
    option :partitions, aliases: %w[-p], required: true, desc: 'new number of partitions'
    def expand(*names)
      names.each do |name|
        opts = options.merge(alter: nil, topic: name)
        topic_options = Kafka::Admin.to_topic_options(opts)
        Kafka::Admin::TopicCommand.alter_topic(zk_client, topic_options)
      end
      zk_client.close
    end

    private

    def zk_client
      @zk_client ||= Kafka::Utils.new_zk_client(options.zookeeper)
    end
  end
end
