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
    option :verbose, aliases: %w[-v], desc: 'Verbose output'
    option :dryrun, aliases: %w[-d], desc: 'Output reassignment plan without executing'
    option :wait, aliases: %w[-w], type: :boolean, desc: 'Wait for all reassignments to finish'
    option :delay, type: :numeric, desc: 'Delay in seconds between continous reassignment iterations, default 5s'
    def migrate_broker
      with_zk_client do |zk_client|
        old_leader, new_leader = options.values_at(:from, :to)
        plan = MigrationPlan.new(zk_client, old_leader, new_leader, log_plan: options.verbose, logger: logger)
        reassigner = create_reassigner(zk_client, options)
        execute_reassignment(reassigner, plan, options)
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
    option :verbose, aliases: %w[-v], desc: 'Verbose output'
    option :dryrun, aliases: %w[-d], desc: 'Output reassignment plan without executing'
    option :wait, aliases: %w[-w], type: :boolean, desc: 'Wait for all reassignments to finish'
    option :delay, type: :numeric, desc: 'Delay in seconds between continous reassignment iterations, default 5s'
    def shuffle(regexp='.*')
      with_zk_client do |zk_client|
        plan_factory = if options.rack_aware
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
          logger: logger,
          log_plan: options.dryrun,
        })
        reassigner = create_reassigner(zk_client, options)
        execute_reassignment(reassigner, plan, options)
      end
    end

    desc 'decommission-broker BROKER_ID', 'Decommission a broker'
    option :limit, aliases: %w[-l], type: :numeric, desc: 'Max number of partitions to reassign at a time'
    option :rendezvous, aliases: %w[-R], type: :boolean, desc: 'Whether to use Rendezvous-hashing'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    option :verbose, aliases: %w[-v], desc: 'Verbose output'
    option :dryrun, aliases: %w[-d], desc: 'Output reassignment plan without executing'
    option :wait, aliases: %w[-w], type: :boolean, desc: 'Wait for all reassignments to finish'
    option :delay, type: :numeric, desc: 'Delay in seconds between continous reassignment iterations, default 5s'
    def decommission_broker(broker_id)
      with_zk_client do |zk_client|
        if options.rendezvous?
          plan = RendezvousShufflePlan.new(zk_client, blacklist: [broker_id.to_i])
        else
          plan = DecommissionPlan.new(zk_client, broker_id.to_i)
        end
        reassigner = create_reassigner(zk_client, options)
        execute_reassignment(reassigner, plan, options)
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

    def execute_reassignment(reassigner, plan, options)
      ReassignmentTask.new(reassigner, plan, shell, logger: logger).execute(options.dryrun)
    end

    def create_reassigner(zk_client, options)
      if options.wait?
        ContinousReassigner.new(zk_client, limit: options.limit, logger: logger, log_assignments: options.verbose, delay: options.delay, shell: shell)
      else
        Reassigner.new(zk_client, limit: options.limit, logger: logger, log_assignments: options.verbose)
      end
    end
  end
end
