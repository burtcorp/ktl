# encoding: utf-8

require 'spec_helper'


describe 'bin/ktl cluster' do
  include_context 'integration setup'

  let :partitions do
    path = Kafka::Utils::ZkUtils.reassign_partitions_path
    fetch_json(path, 'partitions')
  end

  before do
    register_broker(0)
  end

  describe 'stats' do
    before do
      create_topic(%w[topic1 --partitions 2])
      create_partitions('topic1', partitions: 2)
    end

    it 'prints statistics about cluster' do
      output = capture { run(['cluster', 'stats'], zk_args) }
      expect(output).to include('Cluster status:')
      expect(output).to include('topics: 1 (2 partitions)')
      expect(output).to include('brokers: 1')
      expect(output).to include('- 0 leader for 2 partitions (100.00 %)')
    end
  end

  shared_examples 'overflow znodes' do
    context 'when there are overflow znodes present' do
      before do
        overflow_json = {
          version: 1,
          partitions: [
            {topic: 'topic1', partition: 0, replicas: [1]}
          ]
        }.to_json
        ktl_zk.create_persistent_path('/ktl/overflow/0', overflow_json, no_acl)
      end

      before do
        interactive(input) do
          silence { run(['cluster', command], command_args + zk_args) }
        end
      end

      context 'and the user chooses to use overflow data' do
        let :input do
          %w[y]
        end

        it 'uses the overflow data' do
          expect(partitions).to match [
            a_hash_including('topic' => 'topic1', 'partition' => 0, 'replicas' => [1])
          ]
        end
      end

      context 'and the user chooses to not use overflow data' do
        let :input do
          %w[n]
        end

        it 'does not use the overflow data' do
          expect(partitions).to contain_exactly(*reassigned_partitions)
        end
      end
    end
  end

  shared_examples 'progress znodes' do
    context 'znodes for progress state' do
      before do
        silence { run(['cluster', command], command_args + zk_args) }
      end

      it 'writes the reassignment json to a `reassign` state prefix' do
        partitions = fetch_json('/ktl/reassign', 'partitions')
        expect(partitions).to contain_exactly(*reassigned_partitions)
      end
    end
  end

  describe 'migrate-broker' do
    let :command do
      'migrate-broker'
    end

    let :command_args do
      %w[--from 0 --to 1]
    end

    let :reassigned_partitions do
      [
        a_hash_including('topic' => 'topic1', 'partition' => 0, 'replicas' => [1]),
        a_hash_including('topic' => 'topic2', 'partition' => 0, 'replicas' => [1]),
      ]
    end

    before do
      %w[topic1 topic2].each do |topic|
        create_topic(topic)
        create_partitions(topic, isr: [0, 1])
      end
      register_broker(1)
    end

    it 'kick-starts a reassignment command for migrating partitions' do
      silence { run(%w[cluster migrate-broker], command_args + zk_args) }
      expect(partitions).to contain_exactly(*reassigned_partitions)
    end

    include_examples 'overflow znodes'
    include_examples 'progress znodes'
  end

  describe 'preferred-replica' do
    let :partitions do
      path = Kafka::Utils::ZkUtils.preferred_replica_leader_election_path
      fetch_json(path, 'partitions')
    end

    before do
      register_broker(1)
      %w[topic1 topic2].each do |topic|
        create_topic(topic)
        create_partitions(topic, isr: [0, 1])
      end
    end

    it 'kick-starts a preferred replica command' do
      silence { run(%w[cluster preferred-replica], zk_args) }
      expect(partitions).to contain_exactly(
        a_hash_including('topic' => 'topic1', 'partition' => 0),
        a_hash_including('topic' => 'topic2', 'partition' => 0)
      )
    end

    context 'when given a topic regexp' do
      it 'only includes matched topics' do
        silence { run(%w[cluster preferred-replica ^topic1$], zk_args) }
        expect(partitions).to match [
          a_hash_including('topic' => 'topic1', 'partition' => 0)
        ]
        expect(partitions).to_not match [
          a_hash_including('topic' => 'topic2', 'partition' => 0)
        ]
      end
    end

    context 'when given a topic regexp that doesn\'t match anything' do
      it 'prints an error message' do
        output = capture do
          run(%w[cluster preferred-replica ^topics1$], zk_args)
        end
        expect(output).to match /no topics matched/
      end
    end
  end

  describe 'shuffle' do
    before do
      register_broker(1)
      %w[topic1 topic2].each do |topic|
        create_topic(topic, %w[--partitions 2 --replication-factor 2 --replica-assignment 0:1,0:1])
        create_partitions(topic, partitions: 2, isr: [0, 1])
      end
    end

    it 'kick-starts a partition reassignment command' do
      silence { run(%w[cluster shuffle], zk_args) }
      expect(partitions).to_not be_empty
    end

    context 'when given a topic regexp' do
      it 'only includes matched topics' do
        silence { run(%w[cluster shuffle ^topic1$], zk_args) }
        expect(partitions).to match [
          a_hash_including('topic' => 'topic1')
        ]
      end
    end

    context 'when using rendezvous hashing' do
      it 'kick-starts a partition reassignment command' do
        silence { run(%w[cluster shuffle --rendezvous], zk_args) }
        expect(partitions).to_not be_empty
      end
    end

    context 'when given an explicit replication factor' do
      it 'respects it' do
        silence { run(%w[cluster shuffle -r 1], zk_args) }
        partitions.each do |partition|
          replicas = partition['replicas']
          expect(replicas.size).to eq(1)
        end
        expect(partitions).to_not be_empty
      end
    end

    context 'when asked to wait for the reassignment to complete' do
      it 'does not exit until the reassignment has completed' do
        run_in_thread(%w[cluster shuffle --wait], zk_args) do |thread|
          sleep 0.1 until ktl_zk.path_exists('/ktl/reassign')
          expect(fetch_json(Kafka::Utils::ZkUtils.reassign_partitions_path, 'partitions')).to_not be_empty
          expect(thread).to be_alive
          ktl_zk.delete_path(Kafka::Utils::ZkUtils.reassign_partitions_path)
          thread.join
          expect(thread.status).to eq(false)
        end
      end
    end
  end

  describe 'decommission-broker' do
    let :command do
      'decommission-broker'
    end

    let :command_args do
      %w[1]
    end

    let :reassigned_partitions do
      [
        a_hash_including('topic' => 'topic1', 'partition' => 0, 'replicas' => [0, 2]),
        a_hash_including('topic' => 'topic1', 'partition' => 1, 'replicas' => [0, 2]),
        a_hash_including('topic' => 'topic2', 'partition' => 0, 'replicas' => [0, 2]),
        a_hash_including('topic' => 'topic2', 'partition' => 1, 'replicas' => [0, 2]),
      ]
    end

    before do
      register_broker(1)
      register_broker(2)
      %w[topic1 topic2].each do |topic|
        create_topic(topic, %w[--partitions 2 --replication-factor 2 --replica-assignment 0:1,0:1])
        create_partitions(topic, partitions: 2, isr: [0, 1])
      end
    end

    it 'kick-starts a partition reassignment command' do
      silence { run(%w[cluster decommission-broker], command_args + zk_args) }
      expect(partitions).to contain_exactly(*reassigned_partitions)
    end

    include_examples 'overflow znodes'
    include_examples 'progress znodes'
  end

  describe 'progress' do
    let :console_output do
      capture { run(%w[cluster reassignment-progress], command_args + zk_args) }
    end

    let :command_args do
      []
    end

    context 'when there is an active reassignment in progress' do
      before do
        register_broker(1)
        %w[topic1 topic2].each do |topic|
          create_topic(topic, %w[--partitions 2 --replication-factor 2 --replica-assignment 0:1,0:1])
          create_partitions(topic, partitions: 2, isr: [0, 1])
        end
        silence { run(%w[cluster shuffle], zk_args) }
      end

      context 'with -v / --verbose flag' do
        let :command_args do
          %w[-v]
        end

        it 'prints the number of remaining reassignments' do
          expect(console_output).to match('remaining partitions to reassign: 2')
        end

        it 'outputs a table of reassignments' do
          expect(console_output).to match(/topic\s+assignments/)
          expect(console_output.size).to be > 2
        end
      end

      context 'without -v / --verbose flag' do
        it 'prints the number of remaining reassignments' do
          expect(console_output).to include('remaining partitions to reassign: 2')
        end
      end
    end

    context 'when there is no active reassignment in progress' do
      it 'prints a message about it' do
        expect(console_output).to include('no partitions remaining to reassign')
      end
    end
  end
end
