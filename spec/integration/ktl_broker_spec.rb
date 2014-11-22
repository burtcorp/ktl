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

  def fetch_json(zk, path, key=nil)
    d = Kafka::Utils::ZkUtils.read_data(zk, path)._1
    d = JSON.parse(d)
    key ? d[key] : d
  end

  def register_broker(zk, id)
    Kafka::Utils::ZkUtils.register_broker_in_zk(zk, id, 'localhost', 9092, 1, 57476)
  end

  def clear_zk_chroot
    Kafka::Utils::ZkUtils.delete_path_recursive(zk_client, '/ktl-test')
  end

  def setup_zk_chroot
    clear_zk_chroot
    Kafka::Utils::ZkUtils.make_sure_persistent_path_exists(zk_client, '/ktl-test')
    Kafka::Utils::ZkUtils.setup_common_paths(ktl_zk_client)
  end

  def create_partitions(zk, name, partitions=1)
    partitions_path = Kafka::Utils::ZkUtils.get_topic_partitions_path(name)
    Kafka::Utils::ZkUtils.create_persistent_path(zk, partitions_path, '')
    partitions.times.map do |i|
      state_path = [partitions_path, i, 'state'].join('/')
      isr = [0, 1]
      state = {controller_epoch: 1, leader: isr.first, leader_epoch: 1, version: 1, isr: isr}
      Kafka::Utils::ZkUtils.create_persistent_path(zk, state_path, state.to_json)
    end
  end

  before do
    setup_zk_chroot
    register_broker(ktl_zk_client, 0)
  end

  after do
    clear_zk_chroot
    zk_client.close
    ktl_zk_client.close
  end

  describe 'migrate' do
    let :args do
      %w[--from 0 --to 1]
    end

    let :partitions do
      path = Kafka::Utils::ZkUtils.reassign_partitions_path
      fetch_json(ktl_zk_client, path, 'partitions')
    end

    before do
      %w[topic1 topic2].each do |t|
        silence { run(['topic', 'create'], [t] + zk_args) }
        create_partitions(ktl_zk_client, t)
      end
      register_broker(ktl_zk_client, 1)
    end

    before do
      interactive(%w[y]) do
        silence { run(['broker', 'migrate'], args + zk_args) }
      end
    end

    it 'kick-starts a partition reassignment command for migrating topic-partitions tuples' do
      expect(partitions).to contain_exactly(
        a_hash_including('topic' => 'topic1', 'partition' => 0, 'replicas' => [1]),
        a_hash_including('topic' => 'topic2', 'partition' => 0, 'replicas' => [1])
      )
    end
  end

  describe 'preferred-replica' do
    let :partitions do
      path = Kafka::Utils::ZkUtils.preferred_replica_leader_election_path
      fetch_json(ktl_zk_client, path, 'partitions')
    end

    before do
      register_broker(ktl_zk_client, 1)
      %w[topic1 topic2].each do |t|
        silence { run(['topic', 'create'], [t] + zk_args) }
        create_partitions(ktl_zk_client, t)
      end
    end

    it 'kick-starts a preferred replica command' do
      silence { run(['broker', 'preferred-replica'], zk_args) }
      expect(partitions).to contain_exactly(
        a_hash_including('topic' => 'topic1', 'partition' => 0),
        a_hash_including('topic' => 'topic2', 'partition' => 0)
      )
    end

    context 'when given a topic regexp' do
      it 'only includes matched topics' do
        silence { run(['broker', 'preferred-replica', '^topic1$'], zk_args) }
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
        output = capture { run(['broker', 'preferred-replica', '^topics1$'], zk_args) }
        expect(output).to match /no topics matched/
      end
    end
  end

  describe 'balance' do
    let :partitions do
      path = Kafka::Utils::ZkUtils.reassign_partitions_path
      fetch_json(ktl_zk_client, path, 'partitions')
    end

    before do
      register_broker(ktl_zk_client, 1)
      %w[topic1 topic2].each do |t|
        silence { run(['topic', 'create'], [t, '--partitions', '2', '--replication-factor', '2', '--replica-assignment', '0:1,0:1'] + zk_args) }
        create_partitions(ktl_zk_client, t, 2)
      end
    end

    it 'kick-starts a partition reassignment command' do
      interactive(%w[y]) do
        silence { run(['broker', 'balance'], zk_args) }
      end
      expect(partitions).to match [
        a_hash_including('topic' => 'topic1', 'partition' => 1, 'replicas' => [1, 0]),
        a_hash_including('topic' => 'topic2', 'partition' => 0, 'replicas' => [1, 0]),
      ]
    end

    it 'ignores assignments that are identical to current assignments' do
      interactive(%w[y]) do
        silence { run(['broker', 'balance', '^topic1$'], zk_args) }
      end
      expect(partitions).to_not match [
        a_hash_including('topic' => 'topic1', 'partition' => 0, 'replicas' => [0, 1]),
        a_hash_including('topic' => 'topic2', 'partition' => 1, 'replicas' => [0, 1]),
      ]
    end

    context 'when given a topic regexp' do
      it 'only includes matched topics' do
        interactive(%w[y]) do
          silence { run(['broker', 'balance', '^topic1$'], zk_args) }
        end
        expect(partitions).to match [
          a_hash_including('topic' => 'topic1', 'partition' => 1, 'replicas' => [1, 0])
        ]
      end

      it 'ignores assignments that are identical to current assignments' do
        interactive(%w[y]) do
          silence { run(['broker', 'balance', '^topic1$'], zk_args) }
        end
        expect(partitions).to_not match [
          a_hash_including('topic' => 'topic1', 'partition' => 0, 'replicas' => [0, 1]),
        ]
      end
    end
  end

  describe 'decommission' do
    let :partitions do
      path = Kafka::Utils::ZkUtils.reassign_partitions_path
      fetch_json(ktl_zk_client, path, 'partitions')
    end

    before do
      register_broker(ktl_zk_client, 1)
      register_broker(ktl_zk_client, 2)
      %w[topic1 topic2].each do |t|
        silence { run(['topic', 'create'], [t, '--partitions', '2', '--replication-factor', '2', '--replica-assignment', '0:1,0:1'] + zk_args) }
        create_partitions(ktl_zk_client, t, 2)
      end
    end

    it 'kick-starts a partition reassignment command' do
      interactive(%w[y]) do
        silence { run(['broker', 'decommission', '1'], zk_args) }
      end
      expect(partitions).to contain_exactly(
        a_hash_including('topic' => 'topic1', 'partition' => 0, 'replicas' => [0, 2]),
        a_hash_including('topic' => 'topic1', 'partition' => 1, 'replicas' => [0, 2]),
        a_hash_including('topic' => 'topic2', 'partition' => 0, 'replicas' => [0, 2]),
        a_hash_including('topic' => 'topic2', 'partition' => 1, 'replicas' => [0, 2]),
      )
    end
  end
end
