# encoding: utf-8

module Ktl
  class Cluster < Thor
    class_option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'

    desc 'stats', 'show statistics about cluster'
    def stats
      brokers = zk_client.brokers
      partitions = zk_client.all_partitions
      topics = extract_topics(partitions)
      ownership = broker_ownership(partitions)
      say 'Cluster status:'
      say '  topics: %d (%d partitions)' % [topics.size, partitions.size]
      say '  brokers: %d' % [brokers.size]
      brokers.foreach do |broker|
        leader_for = ownership[broker.id]
        share = leader_for.fdiv(partitions.size.to_f) * 100
        say '    - %d (%s) leader for %d partitions (%.2f %%)' % [broker.id, broker.host, leader_for, share]
      end
    rescue => e
      say 'Error: %s (%s)' % [e.message, e.class.name], :red
    ensure
      zk_client.close
    end

    private

    def extract_topics(partitions)
      partitions.map(proc { |tp| tp.topic }, Scala::Collection::Immutable::List.can_build_from).to_seq
    end

    def broker_ownership(partitions)
      result = Hash.new(0)
      leaders = zk_client.leader_and_isr_for(partitions)
      leaders.foreach do |item|
        leader = item._2.leader_and_isr.leader
        result[leader] += 1
      end
      result
    end

    def zk_client
      @zk_client ||= ZookeeperClient.new(options.zookeeper).setup
    end
  end
end
