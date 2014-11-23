# encoding: utf-8

require 'spec_helper'


describe 'bin/ktl topic' do
  include_context 'integration setup'

  before do
    register_broker(0)
    register_broker(1)
  end

  describe 'list' do
    before do
      create_topic('topic1', %w[topic1 --partitions 1 --replication-factor 1])
    end

    it 'lists current topics to $stdout' do
      output = capture { run(%w[topic list], zk_args) }
      expect(output).to match('topic1')
    end
  end

  describe 'create' do
    let :args do
      %w[topic1 --partitions 2 --replication-factor 2]
    end

    before do
      silence { run(%w[topic create], args + zk_args) }
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

    context 'with --replica-assignment' do
      let :args do
        %w[topic1 --partitions 2 --replication-factor 2 --replica-assignment 0:1,1:0]
      end

      it 'uses the given replica assignment' do
        replicas = Kafka::Utils::ZkUtils.get_replicas_for_partition(ktl_zk, 'topic1', 0)
        expect(replicas).to eq(scala_int_list([0, 1]))
        replicas = Kafka::Utils::ZkUtils.get_replicas_for_partition(ktl_zk, 'topic1', 1)
        expect(replicas).to eq(scala_int_list([1, 0]))
      end
    end
  end

  describe 'add-partitions' do
    before do
      create_topic('topic1', %w[topic1 --partitions 1 --replication-factor 2])
    end

    it 'expands the number of partitions for given topic' do
      partitions = Kafka::Utils.get_partitions_for_topic(ktl_zk, 'topic1')
      expect(partitions.size).to eq(1)
      silence { run(%w[topic add-partitions], %w[topic1 --partitions 2] + zk_args) }
      partitions = Kafka::Utils.get_partitions_for_topic(ktl_zk, 'topic1')
      expect(partitions.size).to eq(2)
    end
  end

  describe 'delete' do
    before do
      create_topic(%w[topic1 --partitions 1 --replication-factor 2])
    end

    it 'creates a delete marker for given topic' do
      silence { run(%w[topic delete], %w[topic1] + zk_args) }
      delete_path = Kafka::Utils::ZkUtils.get_delete_topic_path('topic1')
      expect(ktl_zk.exists?(delete_path)).to be true
    end
  end
end
