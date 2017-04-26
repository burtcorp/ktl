# encoding: utf-8

module Ktl
  class MigrationPlan
    def initialize(zk_client, old_replica, new_replica, options = {})
      @zk_client = zk_client
      @old_replica = old_replica
      @new_replica = new_replica
      @options = options
      @logger = options[:logger] || NullLogger.new
      @log_plan = !!options[:log_plan]
    end

    def generate
      plan = Scala::Collection::Map.empty
      topics = @zk_client.all_topics
      p @zk_client.replica_assignment_for_topics(topics)
      assignments = ScalaEnumerable.new(@zk_client.replica_assignment_for_topics(topics))
      assignments.each do |item|
        topic_partition = item.first
        replicas = Scala::Collection::JavaConversions.as_java_iterable(item.last).to_a
        # replicas = item.last.to_a
        p replicas
        if replicas.contain?(@old_replica)
          if replicas.contain?(@new_replica)
            index = replicas.index(@old_replica)
            # new_replicas = replicas.updated(index, @new_replica, CanBuildFrom)
            
            @logger.info "Moving #{topic_partition.topic},#{topic_partition.partition} from #{replicas} to #{new_replicas}" if @log_plan
            plan += Scala::Tuple.new(topic_partition, new_replicas)
          else

          end
        end
      end
      plan
    end
  end
end
