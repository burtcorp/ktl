# encoding: utf-8

require 'spec_helper'


describe 'bin/ktl broker' do
  include_context 'integration setup'

  let :command_args do
    []
  end

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

  describe 'migrate' do
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
      interactive(%w[y]) do
        silence { run(%w[broker migrate], command_args + zk_args) }
      end
      expect(partitions).to contain_exactly(*reassigned_partitions)
    end

    include_examples 'overflow znodes' do
      let :command do
        'migrate'
      end
    end
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

  describe 'balance' do
    let :reassigned_partitions do
      [
        a_hash_including('topic' => 'topic1', 'partition' => 1, 'replicas' => [1, 0]),
        a_hash_including('topic' => 'topic2', 'partition' => 0, 'replicas' => [1, 0]),
      ]
    end

    before do
      register_broker(1)
      %w[topic1 topic2].each do |topic|
        create_topic(topic, %w[--partitions 2 --replication-factor 2 --replica-assignment 0:1,0:1])
        create_partitions(topic, partitions: 2, isr: [0, 1])
      end
    end

    it 'kick-starts a partition reassignment command' do
      interactive(%w[y]) do
        silence { run(%w[broker balance], zk_args) }
      end
      expect(partitions).to match(reassigned_partitions)
    end

    it 'ignores assignments that are identical to current assignments' do
      interactive(%w[y]) do
        silence { run(%w[broker balance ^topic1$], zk_args) }
      end
      expect(partitions).to_not match [
        a_hash_including('topic' => 'topic1', 'partition' => 0, 'replicas' => [0, 1]),
        a_hash_including('topic' => 'topic2', 'partition' => 1, 'replicas' => [0, 1]),
      ]
    end

    context 'when given a topic regexp' do
      it 'only includes matched topics' do
        interactive(%w[y]) do
          silence { run(%w[broker balance ^topic1$], zk_args) }
        end
        expect(partitions).to match [
          a_hash_including('topic' => 'topic1', 'partition' => 1, 'replicas' => [1, 0])
        ]
      end

      it 'ignores assignments that are identical to current assignments' do
        interactive(%w[y]) do
          silence { run(%w[broker balance ^topic1$], zk_args) }
        end
        expect(partitions).to_not match [
          a_hash_including('topic' => 'topic1', 'partition' => 0, 'replicas' => [0, 1]),
        ]
      end
    end

    include_examples 'overflow znodes' do
      let :command do
        'balance'
      end
    end
  end

  describe 'decommission' do
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
      interactive(%w[y]) do
        silence { run(%w[broker decommission], command_args + zk_args) }
      end
      expect(partitions).to contain_exactly(*reassigned_partitions)
    end

    include_examples 'overflow znodes' do
      let :command do
        'decommission'
      end
    end
  end
end
