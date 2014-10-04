# encoding: utf-8

module Ktl
  class Consumer < Thor
    class_option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'

    desc 'lag', 'check lag of a consumer group'
    option :topics, type: :array, aliases: %w[-t], default: [], desc: 'list of topics to include (or all if none given)'
    def lag(group_name)
      args = %W[--zkconnect #{options.zookeeper} --group #{group_name}]
      args << '--topic' << options.topics.join(',') if options.topics.any?
      Kafka::Tools::ConsumerOffsetChecker.main(args.to_java(:String))
    end
  end
end
