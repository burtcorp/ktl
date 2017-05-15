# encoding: utf-8

module Ktl
  class MigrationPlan
    def initialize(zk_client, from_brokers, to_brokers, options = {})
      @zk_client = zk_client
      @from_brokers = from_brokers
      @to_brokers = to_brokers
      if @from_brokers.length != @to_brokers.length
        raise ArgumentError, "Both brokers lists must be of equal length. From: #{@from_brokers}, To: #{@to_brokers}"
      elsif !(@from_brokers & @to_brokers).empty?
        raise ArgumentError, "Broker lists must be mutually exclusive. From: #{@from_brokers}, To: #{@to_brokers}"
      end
      from_racks = from_brokers.map {|broker_id| Kafka::Admin.get_broker_rack(zk_client, broker_id)}
      to_racks = to_brokers.map {|broker_id| Kafka::Admin.get_broker_rack(zk_client, broker_id)}
      if from_racks != to_racks
        raise ArgumentError, "Both broker lists must have the same rack setup. From: #{from_racks}, To: #{to_racks}"
      end
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
        new_replicas = replicas
        @from_brokers.each_with_index do |from_broker, index|
          to_broker = @to_brokers[index]
          if new_replicas.contains?(from_broker)
            replacement_index = new_replicas.index_of(from_broker)
            new_replicas = new_replicas.updated(replacement_index, to_broker, CanBuildFrom)
          end
        end
        if replicas != new_replicas
          @logger.debug "Moving #{topic_partition.topic},#{topic_partition.partition} from #{replicas} to #{new_replicas}" if @log_plan
          plan += Scala::Tuple.new(topic_partition, new_replicas)
        end
      end
      plan
    end
  end
end
