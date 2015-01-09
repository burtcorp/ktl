# encoding: utf-8

require 'spec_helper'


module Ktl
  describe DecommissionPlan do
    let :plan do
      described_class.new(zk_client, broker_id)
    end

    let :zk_client do
      double(:zk_client)
    end

    let :broker_id do
      2
    end

    let :brokers do
      b = Scala::Collection::Immutable::List.empty
      b = b.send('::', to_int(0))
      b = b.send('::', to_int(1))
      b = b.send('::', to_int(2))
      b
    end

    let :partitions do
      ps = Scala::Collection::Immutable::List.empty
      %w[topic1 topic2 topic3].each do |topic|
        [0, 1].each { |p| ps = ps.send('::', Kafka::TopicAndPartition.new(topic, p)) }
      end
      ps
    end

    let :assignments do
      a = Scala::Collection::Mutable::HashMap.new
      a.put(Kafka::TopicAndPartition.new('topic1', to_int(0)), scala_int_list([0, 1]))
      a.put(Kafka::TopicAndPartition.new('topic1', to_int(1)), scala_int_list([1, 0]))
      a.put(Kafka::TopicAndPartition.new('topic2', to_int(0)), scala_int_list([2, 1]))
      a.put(Kafka::TopicAndPartition.new('topic2', to_int(1)), scala_int_list([0, 1]))
      a.put(Kafka::TopicAndPartition.new('topic3', to_int(0)), scala_int_list([1, 0]))
      a.put(Kafka::TopicAndPartition.new('topic3', to_int(1)), scala_int_list([2, 0]))
      a
    end

    describe '#generate' do
      before do
        allow(zk_client).to receive(:broker_ids).and_return(brokers)
        allow(zk_client).to receive(:all_partitions).and_return(partitions)
        allow(zk_client).to receive(:replica_assignment_for_topics).and_return(assignments)
      end

      it 'returns a non-empty Scala::Collection::Immutable::Map object' do
        generated_plan = plan.generate
        expect(generated_plan).to be_a(Scala::Collection::Immutable::Map)
        expect(generated_plan).to_not be_empty
      end

      it 'distributes partitions among the remaining brokers' do
        generated_plan = plan.generate
        expect(generated_plan[Kafka::TopicAndPartition.new('topic2', 0)]).to eq(scala_int_list([0, 1]))
        expect(generated_plan[Kafka::TopicAndPartition.new('topic3', 1)]).to eq(scala_int_list([1, 0]))
      end

      it 'does not include the decommissioned broker in new ARs' do
        generated_plan = plan.generate
        expect(generated_plan).to_not be_empty
        generated_plan.foreach do |tuple|
          expect(tuple._2.contains?(2)).to be false
        end
      end

      context 'when there is an uneven distribution between remaining replicas' do
        let :assignments do
          a = Scala::Collection::Mutable::HashMap.new
          a.put(Kafka::TopicAndPartition.new('topic1', 0), scala_int_list([2, 0]))
          a.put(Kafka::TopicAndPartition.new('topic1', 1), scala_int_list([2, 0]))
          a.put(Kafka::TopicAndPartition.new('topic2', 0), scala_int_list([2, 0]))
          a.put(Kafka::TopicAndPartition.new('topic2', 1), scala_int_list([0, 1]))
          a.put(Kafka::TopicAndPartition.new('topic3', 0), scala_int_list([1, 0]))
          a.put(Kafka::TopicAndPartition.new('topic3', 1), scala_int_list([0, 2]))
          a
        end

        let :brokers do
          b = Scala::Collection::Immutable::List.empty
          b = b.send('::', to_int(0))
          b = b.send('::', to_int(1))
          b = b.send('::', to_int(2))
          b = b.send('::', to_int(3))
          b
        end

        it 'attempts to distribute the affected partitions evenly' do
          generated_plan = plan.generate
          expect(generated_plan[Kafka::TopicAndPartition.new('topic1', 0)]).to eq(scala_int_list([3, 0]))
          expect(generated_plan[Kafka::TopicAndPartition.new('topic1', 1)]).to eq(scala_int_list([1, 0]))
          expect(generated_plan[Kafka::TopicAndPartition.new('topic2', 0)]).to eq(scala_int_list([3, 0]))
          expect(generated_plan[Kafka::TopicAndPartition.new('topic3', 1)]).to eq(scala_int_list([0, 3]))
        end

        it 'does not change partitions that does not include decommissioned broker' do
          generated_plan = plan.generate
          expect(generated_plan.contains?(Kafka::TopicAndPartition.new('topic2', 1))).to be false
          expect(generated_plan.contains?(Kafka::TopicAndPartition.new('topic3', 0))).to be false
        end
      end

      context 'when there isn\'t enough brokers to meet replication factor' do
        let :brokers do
          b = Scala::Collection::Immutable::List.empty
          b = b.send('::', to_int(0))
          b = b.send('::', to_int(1))
          b = b.send('::', to_int(2))
          b = b.send('::', to_int(3))
          b
        end

        let :assignments do
          a = Scala::Collection::Mutable::HashMap.new
          a.put(Kafka::TopicAndPartition.new('topic1', 0), scala_int_list([0, 2, 1, 3]))
          a.put(Kafka::TopicAndPartition.new('topic1', 1), scala_int_list([2, 0, 3, 1]))
          a.put(Kafka::TopicAndPartition.new('topic2', 0), scala_int_list([2, 0]))
          a.put(Kafka::TopicAndPartition.new('topic2', 1), scala_int_list([0, 1]))
          a.put(Kafka::TopicAndPartition.new('topic3', 0), scala_int_list([1, 0]))
          a.put(Kafka::TopicAndPartition.new('topic3', 1), scala_int_list([2, 0]))
          a
        end

        it 'raises an InsufficientBrokersRemainingError' do
          expect { plan.generate }.to raise_error(InsufficientBrokersRemainingError, /\d remaining brokers, \d replicas needed/)
        end
      end
    end
  end
end
