# encoding: utf-8

require 'spec_helper'


module Ktl
  describe ShufflePlan do
    let :plan do
      described_class.new(zk_client, filter)
    end

    let :zk_client do
      double(:zk_client)
    end

    let :filter do
      '.*'
    end

    describe '#generate' do
      let :topics do
        scala_list(%w[topic1 topic2])
      end

      let :brokers do
        scala_int_list([0, 1])
      end

      let :replica_assignments do
        ra = Scala::Collection::Mutable::HashMap.new
        ra.put(Kafka::TopicAndPartition.new('topic1', to_int(0)), scala_int_list([0, 1]))
        ra.put(Kafka::TopicAndPartition.new('topic1', to_int(1)), scala_int_list([0, 1]))
        ra.put(Kafka::TopicAndPartition.new('topic2', to_int(0)), scala_int_list([1, 0]))
        ra.put(Kafka::TopicAndPartition.new('topic2', to_int(1)), scala_int_list([1, 0]))
        ra
      end

      let :topics_partitions do
        tps = Scala::Collection::Mutable::HashMap.new
        tps.put('topic1', scala_int_list([0, 1]))
        tps.put('topic2', scala_int_list([0, 1]))
        tps
      end

      before do
        allow(zk_client).to receive(:all_topics).and_return(topics)
        allow(zk_client).to receive(:partitions_for_topics).with(topics).and_return(topics_partitions)
        allow(zk_client).to receive(:replica_assignment_for_topics).with(topics).and_return(replica_assignments)
        allow(zk_client).to receive(:broker_ids).and_return(brokers)
      end

      it 'fetches partitions for topics' do
        plan.generate
        expect(zk_client).to have_received(:partitions_for_topics).with(topics)
      end

      it 'fetches replica assignments for topics' do
        plan.generate
        expect(zk_client).to have_received(:replica_assignment_for_topics).with(topics)
      end

      it 'returns a Scala Map with assignments' do
        generated_plan = plan.generate
        expect(generated_plan).to be_a(Scala::Collection::Immutable::Map)
        expect(generated_plan.size).to eq(2)
      end

      context 'with a non catch-all filter' do
        let :filter do
          '^topic1$'
        end

        let :filtered_topics do
          scala_list(%w[topic1])
        end

        let :topics_partitions do
          tps = Scala::Collection::Mutable::HashMap.new
          tps.put('topic1', scala_int_list([0, 1]))
          tps
        end

        before do
          allow(zk_client).to receive(:partitions_for_topics).with(filtered_topics).and_return(topics_partitions)
          allow(zk_client).to receive(:replica_assignment_for_topics).with(filtered_topics).and_return(replica_assignments)
        end

        it 'only includes filtered topics' do
          generated_plan = plan.generate
          expect(generated_plan.size).to be >= 1
          generated_plan.foreach do |element|
            expect(element.first.topic).to eq('topic1')
            expect(element.first.topic).to_not eq('topic2')
          end
        end
      end
    end
  end
end
