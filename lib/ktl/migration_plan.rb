# encoding: utf-8

module Ktl
  class MigrationPlan
    def initialize(zk_client, old_leader, new_leader)
      @zk_client = zk_client
      @old_leader = old_leader
      @new_leader = new_leader
    end

    def generate
      plan = Scala::Collection::Map.empty
      partitions = ScalaEnumerable.new(@zk_client.all_partitions)
      partitions.each do |tp|
        replicas = @zk_client.replicas_for_partition(tp.topic, tp.partition)
        if replicas.contains?(@old_leader)
          index = replicas.index_of(@old_leader)
          new_replicas = replicas.updated(index, @new_leader, Scala::Collection::Immutable::List.can_build_from)
          plan += Scala::Tuple.new(tp, new_replicas)
        end
      end
      plan
    end
  end
end
