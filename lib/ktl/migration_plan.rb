# encoding: utf-8

module Ktl
  class MigrationPlan
    def initialize(zk_client, old_replica, new_replica, options = {})
      @zk_client = zk_client
      @old_replica = old_replica
      @new_replica = new_replica
      @logger = options[:logger] || NullLogger.new
      @log_plan = !!options[:log_plan]
    end

    def generate
      plan = Scala::Collection::Map.empty
      topics = @zk_client.all_topics
      assignments = ScalaEnumerable.new(@zk_client.replica_assignment_for_topics(topics))
      assignments.each do |item|
        topic_partition = item.first
        replicas = Scala::Collection::JavaConversions.as_java_iterable(item.last).to_a
        if replicas.include?(@old_replica)
          if replicas.include?(@new_replica)
            new_replicas = replicas.dup
            new_replicas.delete(@old_replica)
            @logger.info "Decreasing #{topic_partition.topic},#{topic_partition.partition} from #{replicas} to #{new_replicas}" if @log_plan
            new_replicas = Scala::Collection::JavaConversions.as_scala_iterable(new_replicas.map {|r| r.to_java}).to_seq
            plan += Scala::Tuple.new(topic_partition, new_replicas)
          else
            new_replicas = replicas.dup
            old_replica_index = new_replicas.index(@old_replica)
            new_replicas.insert(old_replica_index, @new_replica)
            @logger.info "Expanding #{topic_partition.topic},#{topic_partition.partition} from #{replicas} to #{new_replicas}" if @log_plan
            new_replicas = Scala::Collection::JavaConversions.as_scala_iterable(new_replicas.map {|r| r.to_java}).to_seq
            plan += Scala::Tuple.new(topic_partition, new_replicas)
          end
        end
      end
      plan
    end
  end
end
