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
    option :from, aliases: %w[-f], type: :array, required: true, desc: 'Broker IDs to migrate away from'
    option :to, aliases: %w[-t], type: :array, required: true, desc: 'Broker IDs to migrate to'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    option :limit, aliases: %w[-l], type: :numeric, desc: 'Max number of partitions to reassign at a time'
    option :verbose, aliases: %w[-v], desc: 'Verbose output'
    option :dryrun, aliases: %w[-d], desc: 'Output reassignment plan without executing'
    option :wait, aliases: %w[-w], type: :boolean, desc: 'Wait for all reassignments to finish'
    option :delay, type: :numeric, desc: 'Delay in seconds between continous reassignment iterations, default 5s'
    option :multi_step_migration, type: :boolean, default: true, desc: 'Perform migration in multiple steps, mirroring partitions to new brokers before removing the old'
    def migrate_broker
      with_zk_client do |zk_client|
        old_brokers, new_brokers = options.values_at(:from, :to)
        plan = MigrationPlan.new(zk_client, old_brokers.map(&:to_i), new_brokers.map(&:to_i), log_plan: options.verbose, logger: logger)
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
    option :multi_step_migration, type: :boolean, default: true, desc: 'Perform migration in multiple steps, mirroring partitions to new brokers before removing the old'
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

    desc 'calculate-load [REGEXP]', 'Create shuffle plan and calculate projected load per broker'
    option :brokers, type: :array, desc: 'Broker IDs'
    option :blacklist, type: :array, desc: 'Broker IDs to exclude'
    option :rendezvous, aliases: %w[-R], type: :boolean, desc: 'Whether to use Rendezvous-hashing based shuffle'
    option :rack_aware, aliases: %w[-a], type: :boolean, desc: 'Whether to use Rack aware + Rendezvous-hashing based shuffle'
    option :replication_factor, aliases: %w[-r], type: :numeric, desc: 'Replication factor to use'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    def calculate_load(regexp='.*')
      require 'kafka/clients'
      with_zk_client do |zk_client|
        plan_factory = if options.rack_aware
          RackAwareShufflePlan
        elsif options.rendezvous
          RendezvousShufflePlan
        else
          nil
        end
        topics = zk_client.all_topics
        topics = topics.filter { |t| !!t.match(Regexp.new(regexp)) }

        if plan_factory
          plan = plan_factory.new(zk_client, {
            filter: Regexp.new(regexp),
            brokers: options.brokers,
            blacklist: options.blacklist,
            replication_factor: options.replication_factor,
            logger: logger,
            log_plan: options.dryrun,
          })
          planned_replica_assignments = plan.generate(true)
        else
          current_replica_assignments = zk_client.replica_assignment_for_topics(topics)
        end

        topics_partitions = ScalaEnumerable.new(zk_client.partitions_for_topics(topics))
        topics_partitions = topics_partitions.sort_by(&:first)

        bootstrap_servers = []
        brokers = ScalaEnumerable.new(zk_client.utils.get_all_brokers_in_cluster).to_a
        brokers.each do |broker|
          endpoints = ScalaEnumerable.new(broker.end_points).to_a
          endpoints.each do |tuple|
            endpoint = tuple.last
            bootstrap_servers << "#{endpoint.host}:#{endpoint.port}"
          end
        end

        prop = {:group_id => 'ktl', :bootstrap_servers => bootstrap_servers}
        consumer = Kafka::Clients::Consumer.new(prop)

        
        leader_load = Hash.new(0)
        leader_count = Hash.new(0)
        replica_load = Hash.new(0)
        replica_count = Hash.new(0)

        topics_partitions.each do |tp|
          topic, partitions = tp.elements
          $stderr.puts "Processing #{topic}, #{partitions.size} partitions"
          next if topic == '__consumer_offsets'
          partitions.size.times do |partition|
            partition_assignment = replica_assignments.apply(Kafka::TopicAndPartition.new(topic, partition))
            tp = Kafka::Clients::TopicPartition.new(topic, partition)
            consumer.assign([tp])
            last = consumer.position(tp)
            consumer.seek_to_beginning([tp])
            first = consumer.position(tp)
            assignments = Scala::Collection::JavaConversions.as_java_iterable(partition_assignment).to_a
            leader = assignments.shift
            partition_size = (last-first)
            leader_load[leader] += partition_size
            leader_count[leader] += 1
            assignments.each do |replica|
              replica_load[leader] += partition_size
              replica_count[leader] += 1
            end
          end
        end

        leader_load.keys.sort.each do |broker|
          puts [broker, leader_load[broker], leader_count[broker], replica_load[broker], replica_count[broker]].join(";")
        end
          # p 'sup'
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
        ContinousReassigner.new(zk_client, limit: options.limit, logger: logger, log_assignments: options.verbose, delay: options.delay, shell: shell, multi_step_migration: options.multi_step_migration)
      else
        Reassigner.new(zk_client, limit: options.limit, logger: logger, log_assignments: options.verbose, multi_step_migration: options.multi_step_migration)
      end
    end
  end
end
