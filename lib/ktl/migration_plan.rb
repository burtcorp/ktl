# encoding: utf-8

module Ktl
  class MigrationPlan
    def initialize(zk_client, topics_partitions, old_leader, new_leader, zk_utils=Kafka::Utils::ZkUtils)
      @zk_client = zk_client
      @topics_partitions = topics_partitions
      @old_leader = old_leader
      @new_leader = new_leader
      @zk_utils = zk_utils
    end

    def generate
      plan = Scala::Collection::Map.empty
      @topics_partitions.foreach do |tp|
        replicas = @zk_utils.get_replicas_for_partition(@zk_client, tp.topic, tp.partition)
        if replicas.contains?(@old_leader)
          index = replicas.index_of(@old_leader)
          new_replicas = replicas.updated(index, @new_leader, Scala::Collection::Immutable::List.can_build_from)
          plan += Scala::Tuple.new(tp, new_replicas)
        end
      end
      plan
    end

    private

    def get_leader_and_isr_for(tp)
      @zk_utils.get_leader_and_isr_for_partition(@zk_client, tp.topic, tp.partition).get
    end
  end
end
