# encoding: utf-8

module Ktl
  class Cluster < Command
    desc 'stats', 'show statistics about cluster'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    def stats
      with_zk_client do |zk_client|
        task = ClusterStatsTask.new(zk_client, shell)
        task.execute
      end
    end

    desc 'preferred-replica [REGEXP]', 'perform preferred replica leader elections'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    def preferred_replica(regexp='.*')
      with_zk_client do |zk_client|
        regexp = Regexp.new(regexp)
        partitions = zk_client.all_partitions
        partitions = partitions.filter { |tp| !!tp.topic.match(regexp) }.to_set
        if partitions.size > 0
          logger.info 'performing preferred replica leader election on %d partitions' % partitions.size
          Kafka::Admin.preferred_replica(zk_client.raw_client, partitions)
        else
          logger.info 'no topics matched %s' % regexp.inspect
        end
      end
    end

    desc 'migrate', 'migrate partitions from one broker to another'
    option :from, aliases: %w[-f], type: :numeric, required: true, desc: 'broker id of old leader'
    option :to, aliases: %w[-t], type: :numeric, required: true, desc: 'broker id of new leader'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    option :limit, aliases: %w[-l], type: :numeric, desc: 'max number of partitions to reassign'
    def migrate
      with_zk_client do |zk_client|
        old_leader, new_leader = options.values_at(:from, :to)
        plan = MigrationPlan.new(zk_client, old_leader, new_leader)
        reassigner = Reassigner.new(zk_client, limit: options.limit)
        execute_reassignment(reassigner, plan)
      end
    end

    desc 'shuffle [REGEXP]', 'shuffle leaders and replicas for partitions'
    option :brokers, type: :array, desc: 'broker ids'
    option :blacklist, type: :array, desc: 'blacklisted broker ids'
    option :rendezvous, aliases: %w[-R], type: :boolean, desc: 'whether to use Rendezvous-hashing based shuffle'
    option :replication_factor, aliases: %w[-r], type: :numeric, desc: 'replication factor to use'
    option :limit, aliases: %w[-l], type: :numeric, desc: 'max number of partitions to reassign'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    def shuffle(regexp='.*')
      with_zk_client do |zk_client|
        plan_factory = options.rendezvous ? RendezvousShufflePlan : ShufflePlan
        plan = plan_factory.new(zk_client, {
          filter: Regexp.new(regexp),
          brokers: options.brokers,
          blacklist: options.blacklist,
          replication_factor: options.replication_factor,
        })
        reassigner = Reassigner.new(zk_client, limit: options.limit)
        execute_reassignment(reassigner, plan)
      end
    end

    desc 'decommission BROKER_ID', 'decommission a broker'
    option :limit, aliases: %w[-l], type: :numeric, desc: 'max number of partitions to reassign'
    option :rendezvous, aliases: %w[-R], type: :boolean, desc: 'whether to use Rendezvous-hashing'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    def decommission(broker_id)
      with_zk_client do |zk_client|
        if options.rendezvous?
          plan = RendezvousShufflePlan.new(zk_client, blacklist: [broker_id.to_i])
        else
          plan = DecommissionPlan.new(zk_client, broker_id.to_i)
        end
        reassigner = Reassigner.new(zk_client, limit: options.limit)
        execute_reassignment(reassigner, plan)
      end
    end

    desc 'reassignment-progress', 'show progress of latest reassignment'
    option :verbose, aliases: %w[-v], desc: 'verbose output'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'zookeeper uri'
    def reassignment_progress
      with_zk_client do |zk_client|
        progress = ReassignmentProgress.new(zk_client, options.merge(logger: logger))
        progress.display(shell)
      end
    end

    private

    def execute_reassignment(reassigner, plan)
      ReassignmentTask.new(reassigner, plan, shell, logger: logger).execute
    end
  end
end
