# encoding: utf-8

module Ktl
  class Cluster < Command
    desc 'stats', 'show statistics about cluster'
    def stats
      with_zk_client do |zk_client|
        brokers = zk_client.brokers
        partitions = zk_client.all_partitions
        topics = extract_topics(partitions)
        leaders = zk_client.leader_and_isr_for(partitions)
        ownership = broker_ownership(leaders)
        say 'Cluster status:'
        say '  topics: %d (%d partitions)' % [topics.size, partitions.size]
        say '  brokers: %d' % [brokers.size]
        brokers.foreach do |broker|
          leader_for = ownership[broker.id]
          share = leader_for.fdiv(partitions.size.to_f) * 100
          say '    - %d (%s) leader for %d partitions (%.2f %%)' % [broker.id, broker.host, leader_for, share]
        end
      end
    end

    private

    def extract_topics(partitions)
      partitions.map(proc { |tp| tp.topic }, Scala::Collection::Immutable::List.can_build_from).to_seq
    end

    def broker_ownership(leaders)
      result = Hash.new(0)
      leaders.foreach do |item|
        leader = item._2.leader_and_isr.leader
        result[leader] += 1
      end
      result
    end
  end
end
