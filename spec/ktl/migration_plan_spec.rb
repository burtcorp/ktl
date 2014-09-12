# encoding: utf-8

require 'spec_helper'


module Ktl
  describe MigrationPlan do
    let :plan do
      described_class.new(zk_client, topics_partitions, old_leader, new_leader, zk_utils).generate
    end

    let :zk_client do
      double(:zk_client)
    end

    let :owned do
      Kafka::Common::TopicAndPartition.new('test-topic-1', 0)
    end

    let :not_owned do
      Kafka::Common::TopicAndPartition.new('test-topic-1', 1)
    end

    let :topics_partitions do
      l = double(:list)
      allow(l).to receive(:foreach).and_yield(owned).and_yield(not_owned)
      l
    end

    let :old_leader do
      0
    end

    let :new_leader do
      1
    end

    let :zk_utils do
      double(:zk_utils)
    end

    describe '#generate' do
      before do
        allow(zk_utils).to receive(:get_replicas_for_partition).with(zk_client, 'test-topic-1', 0)
          .and_return(FakeSet.new([0, 2]))
        allow(zk_utils).to receive(:get_replicas_for_partition).with(zk_client, 'test-topic-1', 1)
          .and_return(FakeSet.new([2, 1]))
      end

      it 'returns an object with topic-partitions <-> new AR mappings' do
        f = plan.first
        expect(f._1).to be_a(Kafka::Common::TopicAndPartition)
        expect(f._2).to be_a(Array)
      end

      it 'includes the new leader in the AR mapping' do
        f = plan.first
        expect(f._2).to include(1)
      end

      it 'includes the previous ISRs in the new mapping' do
        f = plan.first
        expect(f._2).to include(2)
      end

      it 'skips topic-partitions that are not owned by `old_leader`' do
        expect(plan.contains?(not_owned)).to be false
      end
    end
  end
end
