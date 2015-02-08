# encoding: utf-8

module Ktl
  class Broker < Command
    desc 'migrate', 'migrate partitions from one broker to another'
    option :from, aliases: %w[-f], type: :numeric, required: true, desc: 'broker id of old leader'
    option :to, aliases: %w[-t], type: :numeric, required: true, desc: 'broker id of new leader'
    def migrate
      with_zk_client do |zk_client|
        old_leader, new_leader = options.values_at(:from, :to)
        plan = MigrationPlan.new(zk_client, old_leader, new_leader)
        reassigner = Reassigner.new(:migrate, zk_client)
        task = ReassignmentTask.new(reassigner, plan, shell)
        task.execute
      end
    end

    desc 'preferred-replica [REGEXP]', 'perform preferred replica leader elections'
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

    desc 'shuffle [REGEXP]', 'shuffle leadership and replicas for partitions'
    def shuffle(regexp='.*')
      with_zk_client do |zk_client|
        plan = ShufflePlan.new(zk_client, regexp)
        reassigner = Reassigner.new(:shuffle, zk_client)
        task = ReassignmentTask.new(reassigner, plan, shell)
        task.execute
      end
    end

    desc 'decommission BROKER_ID', 'decommission a broker'
    def decommission(broker_id)
      with_zk_client do |zk_client|
        plan = DecommissionPlan.new(zk_client, broker_id.to_i)
        reassigner = Reassigner.new(:decommission, zk_client)
        task = ReassignmentTask.new(reassigner, plan, shell)
        task.execute
      end
    end

    desc 'progress COMMAND', 'show progress of a reassignment command'
    option :verbose, aliases: %w[-v], desc: 'verbose output'
    def progress(command)
      if %w[migrate shuffle decommission].include?(command)
        with_zk_client do |zk_client|
          progress = ReassignmentProgress.new(zk_client, command, options)
          progress.display(shell);
        end
      else
        raise Thor::Error,
          shell.set_color('Error: ', :red) << %(#{command.inspect} must be one of migrate, shuffle or decommission)
      end
    end
  end
end
