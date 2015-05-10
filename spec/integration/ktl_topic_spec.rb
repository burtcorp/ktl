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

  describe 'describe' do
    before do
      create_topic('topic1', %w[--partitions 2 --replication-factor 2])
      create_partitions('topic1', partitions: 2, isr: [1])
      create_topic('topic2', %w[--partitions 2 --replication-factor 2])
      create_partitions('topic2', partitions: 2, isr: [0, 1])
      create_topic('topic3', %w[--partitions 2 --replication-factor 2])
    end

    context 'without any options' do
      it 'prints information about all topics to $stdout' do
        output = capture { run(%w[topic describe], zk_args) }
        expect(output).to match(/Topic: topic1\s+Partition: 0/)
        expect(output).to match(/Topic: topic1\s+Partition: 1/)
        expect(output).to match(/Topic: topic2\s+Partition: 0/)
        expect(output).to match(/Topic: topic2\s+Partition: 1/)
      end
    end

    context 'with --unavailable option' do
      it 'prints information about unavailable topics to $stdout' do
        output = capture { run(%w[topic describe --unavailable], zk_args) }
        expect(output).to match(/Topic: topic3\s+Partition: 0/)
        expect(output).to match(/Topic: topic3\s+Partition: 1/)
      end
    end

    context 'with --under-replicated option' do
      it 'prints information about under-replicated topics to $stdout' do
        output = capture { run(%w[topic describe --under-replicated], zk_args) }
        expect(output).to match(/Topic: topic1\s+Partition: 0/)
        expect(output).to match(/Topic: topic1\s+Partition: 1/)
      end
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

  describe 'reaper' do
    let :kafkactl do
      Support::KafkaCtl.new(cluster_size: 1, zookeeper_port: 2185)
    end

    before do
      clear_zk_chroot
      kafkactl.start
      create_topic(%w[topic1 --partitions 1])
      create_topic(%w[topic2 --partitions 2])
      create_topic(%w[topic-3 --partitions 3])
      wait_until_topics_exist('localhost:9192', %w[topic1 topic2 topic-3])
    end

    after do
      kafkactl.stop
    end

    it 'creates a delete marker for each empty topic' do
      silence { run(%w[topic reaper ^topic\d$ --delay 0], zk_args) }
      %w[topic1 topic2].each do |topic|
        delete_path = Kafka::Utils::ZkUtils.get_delete_topic_path(topic)
        expect(ktl_zk.exists?(delete_path)).to be true
      end
    end
  end
end
