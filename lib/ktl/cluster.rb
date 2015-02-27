# encoding: utf-8

module Ktl
  class Cluster < Command
    desc 'stats', 'show statistics about cluster'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    def stats
      with_zk_client do |zk_client|
        task = ClusterStatsTask.new(zk_client, shell)
        task.execute
      end
    end
  end
end
