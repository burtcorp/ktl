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
      //
    end

    describe '#generate' do
      let :brokers do
        [0xb1, 0xb2]
      end

      let :replica_count do
        2
      end

      let :assignments do
        {
          'topic1' => [brokers[0, replica_count], brokers[0, replica_count]],
          'topic2' => [brokers[0, replica_count].reverse, brokers[0, replica_count].reverse],
        }
      end

      before do
        allow(zk_client).to receive(:all_topics).and_return(scala_list(assignments.keys))
        allow(zk_client).to receive(:partitions_for_topics) do |scala_topics|
          ScalaEnumerable.new(scala_topics).each_with_object(Scala::Collection::Mutable::HashMap.new) do |topic, scala_topic_partitions|
            scala_topic_partitions.put(topic, scala_int_list(assignments[topic].size.times.to_a))
          end
        end
        allow(zk_client).to receive(:replica_assignment_for_topics) do |scala_topics|
          ScalaEnumerable.new(scala_topics).each_with_object(Scala::Collection::Mutable::HashMap.new) do |topic, scala_assignments|
            assignments.each do |topic, partition_brokers|
              partition_brokers.each_with_index do |brokers, partition|
                scala_assignments.put(Kafka::TopicAndPartition.new(topic, to_int(partition)), scala_int_list(brokers))
              end
            end
          end
        end
        allow(zk_client).to receive(:broker_ids).and_return(scala_int_list(brokers))
      end

      it 'fetches partitions for topics' do
        plan.generate
        expect(zk_client).to have_received(:partitions_for_topics).with(scala_list(assignments.keys))
      end

      it 'fetches replica assignments for topics' do
        plan.generate
        expect(zk_client).to have_received(:replica_assignment_for_topics).with(scala_list(assignments.keys))
      end

      it 'returns a Scala Map with assignments' do
        expect(plan.generate).to be_a(Scala::Collection::Immutable::Map)
      end

      it 'returns a assignments keyed by topic paritions' do
        plan.generate.keys.foreach do |key|
          expect(key).to be_a(Kafka::TopicAndPartition)
        end
      end

      it 'returns a assignments with broker values' do
        plan.generate.values.foreach do |value|
          expect(brokers).to include(*ScalaEnumerable.new(value).to_a)
        end
      end

      it 'does not return topic partitions not needing reassignment' do
        plan.generate.foreach do |tuple|
          expect(ScalaEnumerable.new(tuple.last).to_a).to_not eq(assignments[tuple.first.topic][tuple.first.partition])
        end
      end

      context 'with a non catch-all filter' do
        let :filter do
          /^topic1$/
        end

        it 'fetches replica assignments only for filtered topics' do
          plan.generate
          expect(zk_client).to have_received(:replica_assignment_for_topics).with(scala_list(assignments.keys.grep(filter)))
        end

        it 'does not include topics not included by the filter' do
          plan.generate.foreach do |element|
            expect(element.first.topic).to_not eq('topic2')
          end
        end
      end
    end
  end
end
