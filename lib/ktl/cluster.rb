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
    option :bounded_load, aliases: %w[-b], type: :boolean, desc: 'Whether to use Bounded load Rendezvous-hashing based shuffle'
    option :minimal_movement, aliases: %w[-m], type: :boolean, desc: 'Whether to use Minimal movement based shuffle'
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
        plan_factory = if options.minimal_movement
          MinimalMovementShufflePlan
        elsif options.rack_aware
          RackAwareShufflePlan
        elsif options.bounded_load
          BoundedLoadShufflePlan
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
    option :rendezvous, aliases: %w[-R], type: :boolean, desc: 'Whether to use Rendezvous-hashing based shuffle'
    option :rack_aware, aliases: %w[-a], type: :boolean, desc: 'Whether to use Rack aware + Rendezvous-hashing based shuffle'
    option :replication_factor, aliases: %w[-r], type: :numeric, desc: 'Replication factor to use'
    option :zookeeper, aliases: %w[-z], required: true, desc: 'ZooKeeper URI'
    def calculate_load(regexp='.*')
      require 'kafka/clients'
      require 'digest'
      with_zk_client do |zk_client|
        topics = zk_client.all_topics
        topics = topics.filter { |t| !!t.match(Regexp.new(regexp)) }

        assignments = []
        current_assignment = zk_client.replica_assignment_for_topics(topics)
        assignments << ['Current', current_assignment]

        bootstrap_servers = []
        brokers = ScalaEnumerable.new(zk_client.utils.get_all_brokers_in_cluster).to_a
        rack_mappings = {}
        brokers.each do |broker|
          endpoints = ScalaEnumerable.new(broker.end_points).to_a
          endpoints.each do |tuple|
            endpoint = tuple.last
            bootstrap_servers << "#{endpoint.host}:#{endpoint.port}"
          end
          rack_mappings[broker.id] = Kafka::Admin.get_broker_rack(zk_client, broker.id)
        end
        racks = rack_mappings.values.uniq.sort
        scale_down_brokers = racks.map {|rack| rack_mappings.select {|broker_id, broker_rack| broker_rack == rack}.first.first}
        scale_down_size = brokers.size - scale_down_brokers.size
        plans = [ShufflePlan, RackAwareShufflePlan, BoundedLoadShufflePlan, MinimalMovementShufflePlan]

        plans.each do |plan_factory|
          plan = plan_factory.new(zk_client, {
            filter: Regexp.new(regexp),
            replication_factor: options.replication_factor,
            logger: logger,
            log_plan: false,
          })
          $stderr.puts "Generating #{plan_factory.name}"
          generated_plan = plan.generate(true)
          if scale_down_size >= options.replication_factor
            $stderr.puts "Generating scale down plan (without #{scale_down_brokers})"
            plan = plan_factory.new(zk_client, {
              filter: Regexp.new(regexp),
              blacklist: scale_down_brokers,
              current_assignment: generated_plan,
              replication_factor: options.replication_factor,
              logger: logger,
              log_plan: false,
            })
            generated_scale_down_plan = plan.generate
          end

          assignments << [plan_factory.name, generated_plan, generated_scale_down_plan]
        end

        _, shuffle_plan, _ = assignments.select {|a| a.first == ShufflePlan.name}.first
        if shuffle_plan && scale_down_size >= options.replication_factor
          plan = MinimalMovementShufflePlan.new(zk_client, {
            filter: Regexp.new(regexp),
            blacklist: scale_down_brokers,
            current_assignment: shuffle_plan,
            replication_factor: options.replication_factor,
            logger: logger,
            log_plan: false,
          })
          generated_scale_down_plan = plan.generate

          assignments << ['MinimalMovementFromShuffle', shuffle_plan, generated_scale_down_plan]
        end

        topics_partitions = ScalaEnumerable.new(zk_client.partitions_for_topics(topics))
        topics_partitions = topics_partitions.sort_by(&:first)

        prop = {:group_id => 'ktl', :bootstrap_servers => bootstrap_servers}
        consumer = Kafka::Clients::Consumer.new(prop)
        partition_sizes = {}
        cache_file = '.ktl.partition.cache.' + Digest::MD5.hexdigest("#{options.zookeeper}")
        if File.exists?(cache_file)
          begin
            cached_data = JSON.parse(File.read(cache_file))
            cached_data.each do |partition, size|
              partition_sizes[partition] = size
            end
            $stderr.puts "Read #{cached_data.keys.size} partitions from #{cache_file}"
          rescue JSON::ParserError => e
          end
        else
          $stderr.puts "No such cache file: #{cache_file}"
        end


        assignments.each do |(header, replica_assignments, scale_down_reassignments)|
          puts header
          leader_load = Hash.new(0)
          leader_count = Hash.new(0)
          total_load = Hash.new(0)
          total_count = Hash.new(0)
          rack_leader_load = Hash.new(0)
          rack_leader_count = Hash.new(0)
          rack_total_load = Hash.new(0)
          rack_total_count = Hash.new(0)
          scale_leader_load = 0
          scale_leader_count = 0
          scale_total_load = 0
          scale_total_count = 0
          rebalance_leader_load = 0
          rebalance_leader_count = 0
          rebalance_total_load = 0
          rebalance_total_count = 0

          topics_partitions.each do |tp|
            topic, partitions = tp.elements
            next if topic == '__consumer_offsets'
            $stderr.puts "Processing #{topic}" unless partition_sizes.has_key?("#{topic}:0")
            lookup = false
            partitions.size.times do |partition|
              partition_assignment = Scala::Collection::JavaConversions.as_java_iterable(replica_assignments.apply(Kafka::TopicAndPartition.new(topic, partition))).to_a
              partition_size = partition_sizes["#{topic}:#{partition}"] ||= begin
                lookup = true
                tp = Kafka::Clients::TopicPartition.new(topic, partition)
                consumer.assign([tp])
                last = consumer.position(tp)
                consumer.seek_to_beginning([tp])
                first = consumer.position(tp)
                last - first
              end

              leader = partition_assignment.first
              leader_load[leader] += partition_size
              leader_count[leader] += 1
              rack_leader_load[rack_mappings[leader]] += partition_size
              rack_leader_count[rack_mappings[leader]] += 1
              partition_assignment.each do |replica|
                total_load[replica] += partition_size
                total_count[replica] += 1
                rack_total_load[rack_mappings[replica]] += partition_size
                rack_total_count[rack_mappings[replica]] += 1
              end

              begin
                current_topic_assignment = Scala::Collection::JavaConversions.as_java_iterable(current_assignment.apply(Kafka::TopicAndPartition.new(topic, partition))).to_a
                old_leader = current_topic_assignment.first
                if old_leader != leader
                  if !current_topic_assignment.include?(leader)
                    rebalance_leader_load += partition_size
                  end
                  rebalance_leader_count += 1
                end
                partition_assignment.each do |rebalance_replica|
                  if !current_topic_assignment.include?(rebalance_replica)
                    rebalance_total_load += partition_size
                    rebalance_total_count += 1
                  end
                end
              rescue Java::JavaUtil::NoSuchElementException => e
              end

              if scale_down_reassignments
                begin
                  partition_scale_down_assignment = Scala::Collection::JavaConversions.as_java_iterable(scale_down_reassignments.apply(Kafka::TopicAndPartition.new(topic, partition))).to_a
                  new_leader = partition_scale_down_assignment.first
                  if new_leader != leader
                    if !partition_assignment.include?(new_leader)
                      scale_leader_load += partition_size
                    end
                    scale_leader_count += 1
                  end
                  partition_scale_down_assignment.each do |scale_replica|
                    if !partition_assignment.include?(scale_replica)
                      scale_total_load += partition_size
                      scale_total_count += 1
                    end
                  end
                rescue Java::JavaUtil::NoSuchElementException => e
                end
              end
            end
            File.open(cache_file, 'w') do |f|
              f.puts JSON.dump(partition_sizes)
            end if lookup
          end

          leader_load.keys.sort.each do |broker|
            puts [broker, leader_load[broker], leader_count[broker], total_load[broker], total_count[broker]].join("\t")
          end
          rack_leader_load.keys.sort.each do |rack|
            puts [rack, rack_leader_load[rack], rack_leader_count[rack], rack_total_load[rack], rack_total_count[rack]].join("\t")
          end
          puts ["Rebalance", rebalance_leader_load, rebalance_leader_count, rebalance_total_load, rebalance_total_count].join("\t")
          puts ["Rescale", scale_leader_load, scale_leader_count, scale_total_load, scale_total_count].join("\t") if scale_down_reassignments
          puts
          puts
        end
        File.open(cache_file, 'w') do |f|
          f.puts JSON.dump(partition_sizes)
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
