# encoding: utf-8

module Ktl
  class Cluster < Command
    desc 'stats', 'Show statistics about cluster'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    def stats
      with_zk_client do |zk_client|
        task = ClusterStatsTask.new(zk_client, shell)
        task.execute
      end
    end

    desc 'preferred-replica [REGEXP]', 'Perform preferred replica leader elections'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
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

    desc 'migrate-broker', 'Migrate partitions from one broker to another'
    option :from, aliases: %w[-f], type: :numeric, required: true, desc: 'Broker ID of old leader'
    option :to, aliases: %w[-t], type: :numeric, required: true, desc: 'Broker ID of new leader'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    option :limit, aliases: %w[-l], type: :numeric, desc: 'Max number of partitions to reassign at a time'
    def migrate_broker
      with_zk_client do |zk_client|
        old_leader, new_leader = options.values_at(:from, :to)
        plan = MigrationPlan.new(zk_client, old_leader, new_leader)
        reassigner = Reassigner.new(zk_client, limit: options.limit)
        execute_reassignment(reassigner, plan)
      end
    end

    desc 'shuffle [REGEXP]', 'Shuffle leaders and replicas for partitions'
    option :brokers, type: :array, desc: 'Broker IDs'
    option :blacklist, type: :array, desc: 'Broker IDs to exclude'
    option :rendezvous, aliases: %w[-R], type: :boolean, desc: 'Whether to use Rendezvous-hashing based shuffle'
    option :rack_aware, aliases: %w[-a], type: :boolean, desc: 'Whether to use Rack aware + Rendezvous-hashing based shuffle'
    option :replication_factor, aliases: %w[-r], type: :numeric, desc: 'Replication factor to use'
    option :limit, aliases: %w[-l], type: :numeric, desc: 'Max number of partitions to reassign at a time'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    def shuffle(regexp='.*')
      with_zk_client do |zk_client|
        plan_factory = if options.rackaware
          RackAwareShufflePlan
        elsif options.rendezvous
          RendezvousShufflePlan
        else
          ShufflePlan
        end
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

    desc 'decommission-broker BROKER_ID', 'Decommission a broker'
    option :limit, aliases: %w[-l], type: :numeric, desc: 'Max number of partitions to reassign at a time'
    option :rendezvous, aliases: %w[-R], type: :boolean, desc: 'Whether to use Rendezvous-hashing'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    def decommission_broker(broker_id)
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

    desc 'reassignment-progress', 'Show progress of latest reassignment command'
    option :verbose, aliases: %w[-v], desc: 'Verbose output'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
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
