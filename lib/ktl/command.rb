# encoding: utf-8

module Ktl
  class Command < Thor
    class_option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'

    protected

    def with_zk_client
      zk_client = ZookeeperClient.new(options.zookeeper).setup
      yield zk_client
    rescue => e
      say 'Error: %s (%s)' % [e.message, e.class.name], :red
    ensure
      zk_client.close
    end
  end
end
