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
      create_topic(%w[topic1 --partitions 1 --replication-factor 1])
    end

    it 'lists current topics to $stdout' do
      output = capture { run(%w[topic list], zk_args) }
      expect(output.strip).to eq('topic1')
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
      partitions = ktl_zk.get_partitions_for_topics(scala_list(['topic1']))
      expect(partitions['topic1'].size).to eq(2)
    end

    it 'uses given replication factor' do
      2.times do |index|
        replicas = ktl_zk.get_replicas_for_partition('topic1', index)
        expect(replicas.size).to eq(2)
      end
    end

    context 'with --replica-assignment' do
      let :args do
        %w[topic1 --partitions 2 --replication-factor 2 --replica-assignment 0:1,1:0]
      end

      it 'uses the given replica assignment' do
        replicas = []
        ktl_zk.get_replicas_for_partition('topic1', 0).foreach { |r| replicas << r }
        expect(replicas).to eq([0, 1])
        replicas.clear
        ktl_zk.get_replicas_for_partition('topic1', 1).foreach { |r| replicas << r }
        expect(replicas).to eq([1, 0])
      end
    end

    context 'with --config' do
      let :args do
        %w[topic1 --partitions 2 --replication-factor 2 --config cleanup.policy:compact retention.bytes:1024]
      end

      it 'uses the given configuration for the created topic(s)' do
        config = fetch_json(%(/config/topics/topic1), 'config')
        expect(config).to eq('retention.bytes' => '1024', 'cleanup.policy' => 'compact')
      end
    end
  end

  describe 'add-partitions' do
    before do
      create_topic(%w[topic1 --partitions 1 --replication-factor 2])
    end

    it 'expands the number of partitions for given topic' do
      partitions = ktl_zk.get_partitions_for_topics(scala_list(['topic1']))
      expect(partitions['topic1'].size).to eq(1)
      silence { run(%w[topic add-partitions], %w[topic1 --partitions 2] + zk_args) }
      partitions = ktl_zk.get_partitions_for_topics(scala_list(['topic1']))
      expect(partitions['topic1'].size).to eq(2)
    end
  end

  describe 'delete' do
    before do
      create_topic(%w[topic1 --partitions 1 --replication-factor 2])
    end

    it 'creates a delete marker for given topic' do
      silence { run(%w[topic delete], %w[topic1] + zk_args) }
      delete_path = Kafka::Utils::ZkUtils.get_delete_topic_path('topic1')
      expect(ktl_zk.path_exists?(delete_path)).to be true
    end
  end

  describe 'alter' do
    let :kafka_broker do
      Kafka::Test.create_kafka_server({
        'broker.id' => 1,
        'port' => 9192,
        'zookeeper.connect' => zk_uri + zk_chroot,
      })
    end

    before do
      clear_zk_chroot
      kafka_broker.start
      3.times do |index|
        index += 1
        create_topic(%W[topic-#{index} --partitions #{index}])
      end
      create_topic(%W[other-topic --partitions 1])
      wait_until_topics_exist('localhost:9192', %w[topic-1 topic-2 topic-3 other-topic])
    end

    after do
      kafka_broker.shutdown
    end

    it 'updates configuration for topics matching specified regexp' do
      silence { run(%w[topic alter topic-.+ --add flush.messages:1000], zk_args) }
      %w[topic-1 topic-2 topic-3].each do |topic_name|
        config = fetch_json(%(/config/topics/#{topic_name}), 'config')
        expect(config).to include('flush.messages' => '1000')
      end
      other_config = fetch_json(%(/config/topics/other-topic), 'config')
      expect(other_config).to be_empty
    end

    context 'with --remove' do
      it 'removes specified configuration options from topics' do
        silence { run(%w[topic alter topic-.+ --add flush.messages:1000 retention.bytes:1024], zk_args) }
        silence { run(%w[topic alter topic-.+ --remove flush.messages], zk_args) }
        %w[topic-1 topic-2 topic-3].each do |topic_name|
          config = fetch_json(%(/config/topics/#{topic_name}), 'config')
          expect(config).to eq('retention.bytes' => '1024')
        end
      end
    end

    context 'with --add and --remove' do
      it 'updates specified configuration options for matching topics' do
        silence { run(%w[topic alter topic-.+ --add flush.messages:1000 retention.bytes:1024], zk_args) }
        silence { run(%w[topic alter topic-.+ --add cleanup.policy:compact --remove flush.messages], zk_args) }
        %w[topic-1 topic-2 topic-3].each do |topic_name|
          config = fetch_json(%(/config/topics/#{topic_name}), 'config')
          expect(config).to eq('retention.bytes' => '1024', 'cleanup.policy' => 'compact')
        end
      end
    end
  end
end
