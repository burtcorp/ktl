# encoding: utf-8

module Ktl
  class Consumer < Command
    desc 'lag GROUP_NAME', 'Check lag of a consumer group'
    option :topics, type: :array, aliases: %w[-t], default: [], desc: 'List of topics to include (or all if none given)'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    def lag(group_name)
      args = %W[--zookeeper #{options.zookeeper} --group #{group_name}]
      args << '--topic' << options.topics.join(',') if options.topics.any?
      Kafka::Tools::ConsumerOffsetChecker.main(args.to_java(:String))
    end
  end
end
