# encoding: utf-8

require 'spec_helper'


module Ktl
  describe MigrationPlan do
    let :plan do
      described_class.new(zk_client, old_brokers, new_brokers).generate
    end

    let :zk_client do
      double(:zk_client)
    end

    let :old_brokers do
      [0]
    end

    let :new_brokers do
      [1]
    end

    let :assignment do
      ra = Scala::Collection::Mutable::HashMap.new
      ra.put(Kafka::TopicAndPartition.new('test-topic-1', to_int(0)), scala_int_list([0, 2]))
      ra.put(Kafka::TopicAndPartition.new('test-topic-1', to_int(1)), scala_int_list([2, 1]))
      ra
    end

    describe '#generate' do
      before do
        topics = scala_list(%w[test-topic-1])
        allow(zk_client).to receive(:all_topics).and_return(topics)
        allow(zk_client).to receive(:replica_assignment_for_topics).with(topics).and_return(assignment)
        allow(Kafka::Admin).to receive(:get_broker_rack).and_return('rack')
      end

      it 'returns an object with topic-partitions <-> new AR mappings' do
        f = plan.head
        expect(f.first).to be_a(Kafka::TopicAndPartition)
        expect(f.last).to be_a(Scala::Collection::Mutable::MutableList)
      end

      it 'includes the new leader in the AR mapping' do
        f = plan.head
        expect(f.last.contains?(1)).to be true
      end

      it 'includes the previous ARs in the new mapping' do
        f = plan.head
        expect(f.last.contains?(2)).to be true
      end

      it 'skips topic-partitions that are not owned by `old_leader`' do
        not_owned = Kafka::TopicAndPartition.new('test-topic-1', to_int(1))
        expect(plan.contains?(not_owned)).to be false
      end
    end

    describe '#initialize' do
      it 'raises an exception when broker lists are different length' do
        expect { described_class.new(zk_client, [0], [1, 2]) }.to raise_error(ArgumentError, /must be of equal length/)
      end

      it 'raises an exception when broker lists are not mutually exclusive' do
        expect { described_class.new(zk_client, [0, 1], [1, 2]) }.to raise_error(ArgumentError, /must be mutually exclusive/)
      end

      context 'with rack assignment' do
        before do
          allow(Kafka::Admin).to receive(:get_broker_rack).with(anything, 0).and_return('rack0')
          allow(Kafka::Admin).to receive(:get_broker_rack).with(anything, 1).and_return('rack1')
          allow(Kafka::Admin).to receive(:get_broker_rack).with(anything, 2).and_return('rack0')
          allow(Kafka::Admin).to receive(:get_broker_rack).with(anything, 3).and_return('rack2')
        end

        it 'raises an exception if replacement brokers don\'t have matching rack assignments' do
          expect { described_class.new(zk_client, [0, 1], [2, 3]) }.to raise_error(ArgumentError, /must have the same rack setup/)
        end
      end
    end
  end
end
