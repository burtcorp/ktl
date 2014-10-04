# encoding: utf-8

require 'spec_helper'


describe 'bin/ktl cluster' do
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

  def create_partitions(zk, name, partitions=1)
    partitions_path = Kafka::Utils::ZkUtils.get_topic_partitions_path(name)
    Kafka::Utils::ZkUtils.create_persistent_path(zk, partitions_path, '')
    partitions.times.map do |i|
      state_path = [partitions_path, i, 'state'].join('/')
      state = {controller_epoch: 1, leader: 0, leader_epoch: 1, version: 1, isr: [0]}
      Kafka::Utils::ZkUtils.create_persistent_path(zk, state_path, state.to_json)
    end
  end

  before do
    Kafka::Utils::ZkUtils.delete_path_recursive(control_zk, '/ktl-test')
    Kafka::Utils::ZkUtils.make_sure_persistent_path_exists(control_zk, '/ktl-test')
    Kafka::Utils::ZkUtils.setup_common_paths(ktl_zk)
    Kafka::Utils::ZkUtils.register_broker_in_zk(ktl_zk, 0, 'test-host-0', 9092, 1, 57475)
    silence { run(['topic', 'create'], ['topic1', '--partitions', '2'] + zk_args) }
    create_partitions(ktl_zk, 'topic1', 2)
  end

  after do
    Kafka::Utils::ZkUtils.delete_path_recursive(control_zk, '/ktl-test')
    control_zk.close
    ktl_zk.close
  end

  describe 'stats' do
    it 'prints statistics about cluster' do
      output = capture { run(['cluster', 'stats'], zk_args) }
      expect(output).to include('Cluster status:')
      expect(output).to include('topics: 1 (2 partitions)')
      expect(output).to include('brokers: 1')
      expect(output).to include('- 0 (test-host-0) leader for 2 partitions (100.00 %)')
    end
  end
end
