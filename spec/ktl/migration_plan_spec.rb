# encoding: utf-8

require 'spec_helper'


module Ktl
  describe MigrationPlan do
    let :plan do
      described_class.new(zk_client, old_leader, new_leader).generate
    end

    let :zk_client do
      double(:zk_client)
    end

    let :old_leader do
      0
    end

    let :new_leader do
      1
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
  end
end
