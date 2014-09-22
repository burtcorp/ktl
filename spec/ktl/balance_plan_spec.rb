# encoding: utf-8

require 'spec_helper'


module Ktl
  describe BalancePlan do
    let :plan do
      described_class.new(zk_client, filter, zk_utils)
    end

    let :zk_client do
      double(:zk_client)
    end

    let :filter do
      '.*'
    end

    let :zk_utils do
      double(:zk_utils)
    end

    describe '#generate' do
      let :topics do
        t = Scala::Collection::Mutable::MutableList.empty
        t.send('+=', 'topic1')
        t.send('+=', 'topic2')
        t
      end

      let :brokers do
        b = Scala::Collection::Mutable::MutableList.empty
        b.send('+=', 0.to_java(:Integer))
        b.send('+=', 1.to_java(:Integer))
        b
      end

      let :replica_assignments do
        ra = Scala::Collection::Mutable::HashMap.new
        ra.put(Kafka::TopicAndPartition.new('topic1', 0.to_java(:Integer)), [0, 1].map { |i| java.lang.Integer.new(i) })
        ra.put(Kafka::TopicAndPartition.new('topic1', 1.to_java(:Integer)), [0, 1].map { |i| java.lang.Integer.new(i) })
        ra.put(Kafka::TopicAndPartition.new('topic2', 0.to_java(:Integer)), [1, 0].map { |i| java.lang.Integer.new(i) })
        ra.put(Kafka::TopicAndPartition.new('topic2', 1.to_java(:Integer)), [1, 0].map { |i| java.lang.Integer.new(i) })
        ra
      end

      let :topics_partitions do
        tps = Scala::Collection::Mutable::HashMap.new
        tps.put('topic1', [0, 1].map { |i| java.lang.Integer.new(i) })
        tps.put('topic2', [0, 1].map { |i| java.lang.Integer.new(i) })
        tps
      end

      def make_scala_list(arr)
        l = Scala::Collection::Mutable::MutableList.empty
        arr.each { |i| l.send('+=', i) }
        l
      end

      before do
        allow(zk_utils).to receive(:get_all_topics).with(zk_client).and_return(topics)
        allow(zk_utils).to receive(:get_partitions_for_topics).with(zk_client, topics).and_return(topics_partitions)
        allow(zk_utils).to receive(:get_replica_assignment_for_topics).with(zk_client, topics).and_return(replica_assignments)
        allow(zk_utils).to receive(:get_sorted_broker_list).with(zk_client).and_return(brokers)
      end

      it 'fetches partitions for topics' do
        plan.generate
        expect(zk_utils).to have_received(:get_partitions_for_topics).with(zk_client, topics)
      end

      it 'fetches replica assignments for topics' do
        plan.generate
        expect(zk_utils).to have_received(:get_replica_assignment_for_topics).with(zk_client, topics)
      end

      it 'returns a Scala Map with assignments' do
        generated_plan = plan.generate
        expect(generated_plan).to be_a(Scala::Collection::Immutable::Map)
        expect(generated_plan.size).to eq(4)
        expect(generated_plan[Kafka::TopicAndPartition.new('topic1', 0)]).to eq(make_scala_list([1, 0]))
        expect(generated_plan[Kafka::TopicAndPartition.new('topic1', 1)]).to eq(make_scala_list([0, 1]))
        expect(generated_plan[Kafka::TopicAndPartition.new('topic2', 0)]).to eq(make_scala_list([0, 1]))
        expect(generated_plan[Kafka::TopicAndPartition.new('topic2', 1)]).to eq(make_scala_list([1, 0]))
      end

      it 'returns an (almost) deterministic assignment plan' do
        first_plan = plan.generate
        second_plan = described_class.new(zk_client, filter, zk_utils).generate
        topics.foreach do |t|
          [0, 1].each do |p|
            tp = Kafka::TopicAndPartition.new(t, p)
            expect(first_plan[tp]).to eq(second_plan[tp])
          end
        end
      end

      context 'with a non catch-all filter' do
        let :filter do
          '^topic1$'
        end

        let :filtered_topics do
          t = Scala::Collection::Mutable::MutableList.empty
          t.send('+=', 'topic1')
          t
        end

        let :topics_partitions do
          tps = Scala::Collection::Mutable::HashMap.new
          tps.put('topic1', [0, 1].map { |i| java.lang.Integer.new(i) })
          tps
        end

        before do
          allow(zk_utils).to receive(:get_partitions_for_topics).with(zk_client, filtered_topics).and_return(topics_partitions)
          allow(zk_utils).to receive(:get_replica_assignment_for_topics).with(zk_client, filtered_topics).and_return(replica_assignments)
        end

        it 'only includes filtered topics' do
          generated_plan = plan.generate
          [0, 1].each do |p|
            tp = Kafka::TopicAndPartition.new('topic1', p)
            expect(generated_plan.contains?(tp)).to be true
            expect(generated_plan[tp].size).to be > 1
            tp = Kafka::TopicAndPartition.new('topic2', p)
            expect(generated_plan.contains?(tp)).to be false
          end
        end
      end
    end
  end
end
