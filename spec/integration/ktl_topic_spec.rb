# encoding: utf-8

require 'spec_helper'


describe 'bin/ktl topic' do
  let :zk_host do
    'localhost:2181'
  end

  let :control_zk do
    Kafka::Utils.new_zk_client(zk_host)
  end

  let :ktl_zk do
    Kafka::Utils.new_zk_client(zk_host + '/ktl-test')
  end

  let :zk_args do
    ['-z', zk_host + '/ktl-test']
  end

  def run(command, argv)
    Ktl::Cli.start([command, argv].flatten)
  end

  before do
    Kafka::Utils::ZkUtils.delete_path_recursive(control_zk, '/ktl-test')
    Kafka::Utils::ZkUtils.make_sure_persistent_path_exists(control_zk, '/ktl-test')
    Kafka::Utils::ZkUtils.setup_common_paths(ktl_zk)
    Kafka::Utils::ZkUtils.register_broker_in_zk(ktl_zk, 0, 'localhost', 9092, 1, 57475)
    Kafka::Utils::ZkUtils.register_broker_in_zk(ktl_zk, 1, 'localhost', 9093, 1, 57476)
  end

  after do
    Kafka::Utils::ZkUtils.delete_path_recursive(control_zk, '/ktl-test')
    control_zk.close
    ktl_zk.close
  end

  describe 'list' do
    before do
      args = %w[topic1 --partitions 1 --replication-factor 1] + zk_args
      silence { run(['topic', 'create'], args) }
    end

    it 'lists current topics to $stdout' do
      output = capture { run(['topic', 'list'], zk_args) }
      expect(output).to match('topic1')
    end
  end

  describe 'create' do
    let :args do
      %w[topic1 --partitions 2 --replication-factor 2] + zk_args
    end

    before do
      silence { run(['topic', 'create'], args) }
    end

    it 'creates a new topic' do
      expect(Kafka::Admin::AdminUtils.topic_exists?(ktl_zk, 'topic1')).to be true
    end

    it 'uses given number of partitions' do
      partitions = Kafka::Utils.get_partitions_for_topic(ktl_zk, 'topic1')
      expect(partitions.size).to eq(2)
    end

    it 'uses given replication factor' do
      2.times do |i|
        replicas = Kafka::Utils::ZkUtils.get_replicas_for_partition(ktl_zk, 'topic1', i)
        expect(replicas.size).to eq(2)
      end
    end
  end

  describe 'expand' do
    before do
      silence { run(['topic', 'create'], %w[topic1 --partitions 1 --replication-factor 2] + zk_args) }
    end

    it 'expands the number of partitions for given topic' do
      partitions = Kafka::Utils.get_partitions_for_topic(ktl_zk, 'topic1')
      expect(partitions.size).to eq(1)
      capture { run(['topic', 'expand'], %w[topic1 --partitions 2] + zk_args) }
      partitions = Kafka::Utils.get_partitions_for_topic(ktl_zk, 'topic1')
      expect(partitions.size).to eq(2)
    end
  end
end
