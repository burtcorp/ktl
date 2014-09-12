# encoding: utf-8

module Ktl
  class Broker < Thor
    class_option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'

    desc 'migrate', 'migrate topics from one broker to another'
    option :from, aliases: %w[-f], type: :numeric, required: true, desc: 'broker id of old leader'
    option :to, aliases: %w[-t], type: :numeric, required: true, desc: 'broker id of new leader'
    def migrate
      old_leader, new_leader = options.values_at(:from, :to)
      migration_plan = MigrationPlan.new(zk_client, all_topics_partitions, old_leader, new_leader)
      migration_plan = migration_plan.generate
      say 'Moving %d topics and partitions from %d to %d' % [migration_plan.size, old_leader, new_leader]
      Kafka::Admin.reassign_partitions(zk_client, migration_plan)
      zk_client.close
    end

    desc 'preferred-replica', 'perform preferred replica leader elections'
    def preferred_replica(regexp='.*')
      regexp = Regexp.new(regexp)
      topics_partitions = all_topics_partitions.filter { |tp| !!tp.topic.match(regexp) }.to_set
      say 'performing preferred replica leader election on %d topic-partition combinations' % topics_partitions.size
      Kafka::Admin.preferred_replica(zk_client, topics_partitions)
      zk_client.close
    end

    private

    def all_topics_partitions
      @all_topics_partitions ||= Kafka::Utils::ZkUtils.get_all_partitions(zk_client).iterator
    end

    def zk_client
      @zk_client ||= Kafka::Utils.new_zk_client(options.zookeeper)
    end
  end
end
