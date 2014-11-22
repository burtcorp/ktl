# encoding: utf-8

module Ktl
  class DecommissionPlan
    def initialize(zk_client, broker_id)
      @zk_client = zk_client
      @broker_id = broker_id
      @replicas_count = Hash.new(0)
      @leaders_count = Hash.new(0)
    end

    def generate
      plan = Scala::Collection::Map.empty
      brokers = @zk_client.broker_ids
      brokers = brokers - @broker_id
      partitions = @zk_client.all_partitions
      topics = topics_from(partitions)
      assignments = @zk_client.replica_assignment_for_topics(topics)
      count_leaders_and_replicas(@zk_client.leader_and_isr_for(partitions))
      partitions = ScalaEnumerable.new(partitions).sort_by { |tp| tp.topic + tp.partition.to_s }
      partitions.each do |tp|
        replicas = assignments[tp]
        if replicas.contains?(@broker_id)
          if brokers.size >= replicas.size
            brokers_diff = ScalaEnumerable.new(brokers.diff(replicas)).sort
            broker_index = replicas.index_of(@broker_id)
            new_broker = elect_new_broker(broker_index, brokers_diff)
            new_replicas = replicas.updated(broker_index, new_broker, CAN_BUILD_FROM)
            plan += Scala::Tuple.new(tp, new_replicas)
          else
            raise InsufficientBrokersRemainingError, %(#{brokers.size} remaining brokers, #{replicas.size} replicas needed)
          end
        end
      end
      plan
    end

    private

    CAN_BUILD_FROM = Scala::Collection::Immutable::List.can_build_from

    def elect_new_broker(broker_index, diff)
      if broker_index.zero?
        new_broker = diff.min_by { |broker| @replicas_count[broker] }
        @replicas_count[new_broker] += 1
      else
        new_broker = diff.min_by { |broker| @leaders_count[broker] }
        @leaders_count[new_broker] += 1
      end
      new_broker
    end

    def count_leaders_and_replicas(map)
      map.foreach do |element|
        isr = element.last.leader_and_isr.isr
        leader = element.last.leader_and_isr.leader
        isr.foreach do |broker|
          if broker == leader
            @leaders_count[broker] += 1
          else
            @replicas_count[broker] += 1
          end
        end
      end
    end

    def topics_from(partitions)
      partitions.map(proc { |tp| tp.topic }, Scala::Collection::Immutable::List.can_build_from).to_seq
    end
  end
end
