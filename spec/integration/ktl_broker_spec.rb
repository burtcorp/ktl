# encoding: utf-8

require 'spec_helper'


describe 'bin/ktl broker' do
  let :zk do
    'localhost:2181'
  end

  let :zk_client do
    Kafka::Utils.new_zk_client(zk)
  end

  let :ktl_zk_client do
    Kafka::Utils.new_zk_client(zk + '/ktl-test')
  end

  let :zk_args do
    ['-z', zk + '/ktl-test']
  end

  def run(command, argv)
    Ktl::Cli.start([command, argv].flatten)
  end

  before do
    Kafka::Utils::ZkUtils.delete_path_recursive(zk_client, '/ktl-test')
    Kafka::Utils::ZkUtils.make_sure_persistent_path_exists(zk_client, '/ktl-test')
    Kafka::Utils::ZkUtils.setup_common_paths(ktl_zk_client)
    Kafka::Utils::ZkUtils.register_broker_in_zk(ktl_zk_client, 0, 'localhost', 9092, 1, 57475)
  end

  after do
    Kafka::Utils::ZkUtils.delete_path_recursive(zk_client, '/ktl-test')
    zk_client.close
    ktl_zk_client.close
  end

  describe 'migrate' do
    let :args do
      %w[--from 0 --to 1]
    end

    let :data do
      d = Kafka::Utils::ZkUtils.read_data(ktl_zk_client, Kafka::Utils::ZkUtils.reassign_partitions_path)._1
      d = JSON.parse(d)
      d
    end

    before do
      %w[topic1 topic2].each do |t|
        silence { run(['topic', 'create'], [t] + zk_args) }
        partitions_path = Kafka::Utils::ZkUtils.get_topic_partitions_path(t)
        Kafka::Utils::ZkUtils.create_persistent_path(ktl_zk_client, partitions_path, '')
        state_path = partitions_path + '/0/state'
        state = {controller_epoch: 1, leader: 0, leader_epoch: 1, version: 1, isr: [0]}
        Kafka::Utils::ZkUtils.create_persistent_path(ktl_zk_client, state_path, state.to_json)
      end
      Kafka::Utils::ZkUtils.register_broker_in_zk(ktl_zk_client, 1, 'localhost', 9093, 1, 57476)
    end

    before do
      silence { run(['broker', 'migrate'], args + zk_args) }
    end

    it 'kick-starts a partition reassignment command for migrating topic-partitions tuples' do
      expect(data).to have_key('partitions')
      partition_data = data['partitions']
      first, last = partition_data
      expect(first).to include('topic' => 'topic1')
      expect(first).to include('replicas' => [1])
      expect(last).to include('topic' => 'topic2')
      expect(last).to include('replicas' => [1])
    end
  end

  describe 'preferred-replica' do
    let :data do
      zk_path = Kafka::Utils::ZkUtils.preferred_replica_leader_election_path
      d = Kafka::Utils::ZkUtils.read_data(ktl_zk_client, zk_path)._1
      d = JSON.parse(d)
      d
    end

    before do
      Kafka::Utils::ZkUtils.register_broker_in_zk(ktl_zk_client, 1, 'localhost', 9093, 1, 57476)
      %w[topic1 topic2].each do |t|
        silence { run(['topic', 'create'], [t] + zk_args) }
        partitions_path = Kafka::Utils::ZkUtils.get_topic_partitions_path(t)
        Kafka::Utils::ZkUtils.create_persistent_path(ktl_zk_client, partitions_path, '')
        state_path = partitions_path + '/0/state'
        state = {controller_epoch: 1, leader: 0, leader_epoch: 1, version: 1, isr: [0, 1]}
        Kafka::Utils::ZkUtils.create_persistent_path(ktl_zk_client, state_path, state.to_json)
      end
    end

    it 'kick-starts a preferred replica command' do
      silence { run(['broker', 'preferred-replica'], zk_args) }
      partitions = data['partitions']
      expect(partitions).to include({'topic' => 'topic1', 'partition' => 0})
      expect(partitions).to include({'topic' => 'topic2', 'partition' => 0})
    end

    context 'when given a topic regexp' do
      it 'only includes matched topics' do
        silence { run(['broker', 'preferred-replica', '^topic1$'], zk_args) }
        partitions = data['partitions']
        expect(partitions).to include({'topic' => 'topic1', 'partition' => 0})
        expect(partitions).to_not include({'topic' => 'topic2', 'partition' => 0})
      end
    end
  end
end
