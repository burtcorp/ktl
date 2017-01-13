# encoding: utf-8

module Ktl
  class ClusterStatsTask
    def initialize(zk_client, shell)
      @zk_client = zk_client
      @shell = shell
    end

    def execute
      brokers = @zk_client.brokers
      partitions = @zk_client.all_partitions
      topics = extract_topics(partitions)
      leaders = @zk_client.leader_and_isr_for(partitions)
      ownership = broker_ownership(leaders)
      @shell.say 'Cluster status:'
      @shell.say '  topics: %d (%d partitions)' % [topics.size, partitions.size]
      @shell.say '  brokers: %d' % [brokers.size]
      brokers.foreach do |broker|
        leader_for = ownership[broker.id]
        share = leader_for.fdiv(partitions.size.to_f) * 100
        @shell.say '    - %d leader for %d partitions (%.2f %%)' % [broker.id, leader_for, share]
      end
    end

    private

    def extract_topics(partitions)
      partitions.map(proc { |tp| tp.topic }, CanBuildFrom).to_seq
    end

    def broker_ownership(leaders)
      result = Hash.new(0)
      leaders.foreach do |item|
        leader = item.last.leader_and_isr.leader
        result[leader] += 1
      end
      result
    end
  end
end
