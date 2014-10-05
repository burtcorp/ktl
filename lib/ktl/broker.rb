# encoding: utf-8

module Ktl
  class Broker < Command
    desc 'migrate', 'migrate partitions from one broker to another'
    option :from, aliases: %w[-f], type: :numeric, required: true, desc: 'broker id of old leader'
    option :to, aliases: %w[-t], type: :numeric, required: true, desc: 'broker id of new leader'
    def migrate
      with_zk_client do |zk_client|
        old_leader, new_leader = options.values_at(:from, :to)
        migration_plan = MigrationPlan.new(zk_client, old_leader, new_leader).generate
        say 'moving %d partitions from %d to %d' % [migration_plan.size, old_leader, new_leader]
        Kafka::Admin.reassign_partitions(zk_client.raw_client, migration_plan)
      end
    end

    desc 'preferred-replica', 'perform preferred replica leader elections'
    def preferred_replica(regexp='.*')
      with_zk_client do |zk_client|
        regexp = Regexp.new(regexp)
        partitions = zk_client.all_partitions
        partitions = partitions.filter { |tp| !!tp.topic.match(regexp) }.to_set
        if partitions.size > 0
          say 'performing preferred replica leader election on %d partitions' % partitions.size
          Kafka::Admin.preferred_replica(zk_client.raw_client, partitions)
        else
          say 'no topics matched %s' % regexp.inspect
        end
      end
    end

    desc 'balance', 'balance topics and partitions between brokers'
    def balance(regexp='.*')
      with_zk_client do |zk_client|
        plan = BalancePlan.new(zk_client, regexp).generate
        say 'reassigning %d partitions' % plan.size
        Kafka::Admin.reassign_partitions(zk_client.raw_client, plan)
      end
    end
  end
end
