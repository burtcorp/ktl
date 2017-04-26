# encoding: utf-8

module Ktl
  class MigrationPlan
    def initialize(zk_client, old_leader, new_leader, options = {})
      @zk_client = zk_client
      @old_leader = old_leader.to_java
      @new_leader = new_leader.to_java
      @options = options
      @logger = options[:logger] || NullLogger.new
      @log_plan = !!options[:log_plan]
    end

    def generate
      plan = Scala::Collection::Map.empty
      topics = @zk_client.all_topics
      assignments = ScalaEnumerable.new(@zk_client.replica_assignment_for_topics(topics))
      assignments.each do |item|
        topic_partition = item.first
        replicas = item.last
        if replicas.contains?(@old_leader)
          index = replicas.index_of(@old_leader)
          new_replicas = replicas.updated(index, @new_leader, CanBuildFrom)
          @logger.info "Moving #{topic_partition.topic},#{topic_partition.partition} from #{replicas} to #{new_replicas}" if @log_plan
          plan += Scala::Tuple.new(topic_partition, new_replicas)
        end
      end
      plan
    end
  end
end
