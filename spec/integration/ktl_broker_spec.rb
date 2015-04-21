# encoding: utf-8

require 'spec_helper'


describe 'bin/ktl broker' do
  include_context 'integration setup'

  let :partitions do
    path = Kafka::Utils::ZkUtils.reassign_partitions_path
    fetch_json(path, 'partitions')
  end

  before do
    register_broker(0)
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
        Kafka::Utils::ZkUtils.create_persistent_path(ktl_zk, %(/ktl/overflow/#{command}/0), overflow_json)
      end

      before do
        interactive(input) do
          silence { run(['broker', command], command_args + zk_args) }
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
        silence { run(['broker', command], command_args + zk_args) }
      end

      it 'writes the reassignment json to a `reassign` state prefix' do
        indices = ktl_zk.get_children(%(/ktl/reassign/#{command}))
        partitions = fetch_json(%(/ktl/reassign/#{command}), 'partitions')
        expect(partitions).to contain_exactly(*reassigned_partitions)
      end
    end
  end

  describe 'migrate' do
    let :command do
      'migrate'
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
      silence { run(%w[broker migrate], command_args + zk_args) }
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
      silence { run(%w[broker preferred-replica], zk_args) }
      expect(partitions).to contain_exactly(
        a_hash_including('topic' => 'topic1', 'partition' => 0),
        a_hash_including('topic' => 'topic2', 'partition' => 0)
      )
    end

    context 'when given a topic regexp' do
      it 'only includes matched topics' do
        silence { run(%w[broker preferred-replica ^topic1$], zk_args) }
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
          run(%w[broker preferred-replica ^topics1$], zk_args)
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
      silence { run(%w[broker shuffle], zk_args) }
      expect(partitions).to_not be_empty
    end

    context 'when given a topic regexp' do
      it 'only includes matched topics' do
        silence { run(%w[broker shuffle ^topic1$], zk_args) }
        expect(partitions).to match [
          a_hash_including('topic' => 'topic1')
        ]
      end
    end

    context 'when using rendezvous hashing' do
      it 'kick-starts a partition reassignment command' do
        silence { run(%w[broker shuffle --rendezvous], zk_args) }
        expect(partitions).to_not be_empty
      end
    end
  end

  describe 'decommission' do
    let :command do
      'decommission'
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
      silence { run(%w[broker decommission], command_args + zk_args) }
      expect(partitions).to contain_exactly(*reassigned_partitions)
    end

    include_examples 'overflow znodes'
    include_examples 'progress znodes'
  end

  describe 'progress' do
    let :console_output do
      capture { run(%w[broker progress], command_args + zk_args) }
    end

    let :command_args do
      %w[shuffle]
    end

    context 'when there is an active reassignment in progress' do
      before do
        register_broker(1)
        %w[topic1 topic2].each do |topic|
          create_topic(topic, %w[--partitions 2 --replication-factor 2 --replica-assignment 0:1,0:1])
          create_partitions(topic, partitions: 2, isr: [0, 1])
        end
        silence { run(%w[broker shuffle], zk_args) }
      end

      context 'with -v / --verbose flag' do
        let :command_args do
          %w[shuffle -v]
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

    context 'when called with an invalid command' do
      it 'prints an error message' do
        console_output = capture(:stderr) { run(%w[broker progress], %w[hello] + zk_args) }
        expect(console_output).to match('Error: "hello" must be one of migrate, shuffle or decommission')
      end
    end
  end
end
