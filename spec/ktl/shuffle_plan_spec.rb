# encoding: utf-8

require 'spec_helper'


module Ktl
  shared_examples 'a shuffle plan' do
    let :plan do
      described_class.new(zk_client, options)
    end

    let :zk_client do
      double(:zk_client, utils: zk_utils)
    end

    let :zk_utils do
      double(:zk_utils)
    end

    let :options do
      {}
    end

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
        'topic3' => [brokers.reverse[0, replica_count], brokers.reverse[0, replica_count]],
      }
    end

    let :zk_utils do
      double(:zk_utils)
    end

    def generate_broker_metadata(broker_id)
      @brokers[broker_id] ||= begin
        index = broker_id % 10
        rack_name = "rack-#{index}"
        rack = Scala::Option.apply(rack_name.to_java)
        Kafka::Admin::BrokerMetadata.new(broker_id, rack)
      end
    end

    def each_reassignment(scala_reassignments)
      ScalaEnumerable.new(scala_reassignments).each_with_object({}) do |t, result|
        yield t.first.topic, t.first.partition, ScalaEnumerable.new(t.last).to_a
      end
    end

    def apply_reassignments(scala_reassignments)
      each_reassignment(scala_reassignments) do |topic, partition, brokers|
        assignments[topic][partition] = brokers
      end
    end

    before do
      @brokers = {}
      allow(zk_client).to receive(:utils).and_return(zk_utils)
      allow(Kafka::Admin).to receive(:get_broker_metadatas) do |zk_client, broker_list|
        broker_list.map do |broker|
          generate_broker_metadata(broker)
        end
      end
    end

    before do
      allow(zk_client).to receive(:all_topics) do
        scala_list(assignments.keys)
      end
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
      allow(zk_client).to receive(:broker_ids) do
        scala_int_list(brokers)
      end
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

    it 'returns an assignment keyed by topic paritions' do
      plan.generate.keys.foreach do |key|
        expect(key).to be_a(Kafka::TopicAndPartition)
      end
    end

    it 'returns an assignment with broker values' do
      plan.generate.values.foreach do |value|
        expect(brokers).to include(*ScalaEnumerable.new(value).to_a)
      end
    end

    it 'returns unique broker values' do
      plan.generate.values.foreach do |value|
        expect(brokers).to eq(brokers.uniq)
      end
    end

    it 'does not return topic partitions not needing reassignment' do
      plan.generate.foreach do |tuple|
        expect(ScalaEnumerable.new(tuple.last).to_a).to_not eq(assignments[tuple.first.topic][tuple.first.partition])
      end
    end

    it 'raises an error if too few brokers' do
      assignments # force evaluation
      brokers.clear
      expect do
        plan.generate
      end.to raise_error(ArgumentError, /replication factor: #{replica_count} larger than available brokers: 0/)
    end

    context 'with a non catch-all filter' do
      let :options do
        super.merge(filter: /^topic1$/)
      end

      it 'fetches replica assignments only for filtered topics' do
        plan.generate
        expect(zk_client).to have_received(:replica_assignment_for_topics).with(scala_list(assignments.keys.grep(options[:filter])))
      end

      it 'does not include topics not included by the filter' do
        plan.generate.foreach do |element|
          expect(element.first.topic).to_not eq('topic2')
        end
      end
    end

    context 'with custom broker selection' do
      let :options do
        super.merge(brokers: [0xb7, 0xb8])
      end

      it 'generates mappings with only particular brokers' do
        plan.generate.values.foreach do |value|
          expect(ScalaEnumerable.new(value).to_a).to contain_exactly(0xb7, 0xb8)
        end
      end
    end

    context 'with blacklisted brokers' do
      before do
        brokers << 0xb3
      end

      let :options do
        super.merge(blacklist: 0xb1)
      end

      it 'generates mappings without blacklisted brokers' do
        plan.generate.values.foreach do |value|
          expect(ScalaEnumerable.new(value).to_a).to_not include(0xb1)
        end
      end
    end

    context 'with explicit replication factor' do
      let :options do
        super.merge(replication_factor: 1)
      end

      it 'respects it' do
        plan.generate.values.foreach do |value|
          expect(value.size).to eq(1)
        end
      end
    end

    context 'generate_for_new_topic' do
      let :options do
        super.merge(replication_factor: replica_count)
      end

      it 'generates a nested list of broker ids for a new topic' do
        assignments.each do |topic, assignment|
          generated_plan = plan.generate_for_new_topic(topic, assignment.size)
          expect(generated_plan.size).to eql(assignment.size)
          generated_plan.each do |partition|
            expect(partition.uniq.size).to eql(replica_count)
          end
        end
      end
    end
  end

  describe ShufflePlan do
    describe '#generate' do
      include_examples 'a shuffle plan'
    end
  end

  describe RendezvousShufflePlan do
    describe '#generate' do
      include_examples 'a shuffle plan'

      context 'when adding brokers' do
        before do
          apply_reassignments(plan.generate)
          brokers << 0xb3
        end

        it 'does not reassign leader to anything but the new broker' do
          each_reassignment(plan.generate) do |topic, partition, brokers|
            expect(brokers[0]).to satisfy { |leader| leader == 0xb3 || leader == assignments[topic][partition][0] }
          end
        end

        it 'demotes remaining brokers if new leader elected' do
          each_reassignment(plan.generate) do |topic, partition, brokers|
            if brokers[0] == 0xb3
              expect(brokers.drop(1)).to eq(assignments[topic][partition].take(replica_count-1))
            end
          end
        end

        it 'does not reassign followers to anything but the new broker' do
          each_reassignment(plan.generate) do |topic, partition, brokers|
            unless brokers[0] == 0xb3
              expect(brokers[1]).to satisfy { |follower| follower == 0xb3 || follower == assignments[topic][partition][1] }
            end
          end
        end
      end
    end
  end

  describe RackAwareShufflePlan do
    describe '#generate' do
      include_examples 'a shuffle plan'

      context 'when adding brokers' do
        before do
          apply_reassignments(plan.generate)
          brokers << 0xb3
        end

        it 'does not reassign leader to anything but the new broker' do
          each_reassignment(plan.generate) do |topic, partition, brokers|
            expect(brokers[0]).to satisfy { |leader| leader == 0xb3 || leader == assignments[topic][partition][0] }
          end
        end

        it 'demotes remaining brokers if new leader elected' do
          each_reassignment(plan.generate) do |topic, partition, brokers|
            if brokers[0] == 0xb3
              expect(brokers.drop(1)).to eq(assignments[topic][partition].take(replica_count-1))
            end
          end
        end

        it 'does not reassign followers to anything but the new broker' do
          each_reassignment(plan.generate) do |topic, partition, brokers|
            unless brokers[0] == 0xb3
              expect(brokers[1]).to satisfy { |follower| follower == 0xb3 || follower == assignments[topic][partition][1] }
            end
          end
        end
      end

      context 'with multiple brokers per rack' do
        let :assignments do
          {
            'topic1' => [[301], [302], [303]],
            'topic2' => [[301], [302], [303]],
            'topic3' => [[301], [302], [303]],
          }
        end

        let :brokers do
          [201, 202, 203, 101, 102, 103]
        end

        let :replica_count do
          3
        end

        let :options do
          {
            replication_factor: replica_count
          }
        end

        it 'chooses one broker per rack' do
          each_reassignment(plan.generate) do |topic, partition, brokers|
            racks = brokers.map { |broker| generate_broker_metadata(broker).rack.get }
            expect(racks.uniq.size).to eql(3)
          end
        end

        it 'raises exception if broker is missing rack configuration' do
          broker_metadata = generate_broker_metadata(203)
          allow(broker_metadata.rack).to receive(:defined?).and_return(false)
          expect { plan.generate }.to raise_error /Broker 203 is missing rack information/
        end
      end

      context 'with broker missing rack' do
        let :brokers do
          [101, 102, 103]
        end

        before do
          rack = Scala::Option.apply(nil.to_java)
          @brokers[101] = Kafka::Admin::BrokerMetadata.new(101, rack)
        end

        it 'raises exception' do
          expect { plan.generate }.to raise_error(RuntimeError, /Broker 101 is missing rack information/)
        end
      end
    end
  end

  describe MinimalMovementShufflePlan do
    describe '#generate' do
      include_examples 'a shuffle plan'

      context 'with multiple brokers per rack' do
        let :assignments do
          {
            'topic1' => [[301], [302], [303]],
            'topic2' => [[301], [302], [303]],
            'topic3' => [[301], [302], [303]],
          }
        end

        let :brokers do
          [201, 202, 203, 101, 102, 103]
        end

        let :replica_count do
          3
        end

        let :options do
          {
            replication_factor: replica_count
          }
        end

        it 'chooses one broker per rack' do
          each_reassignment(plan.generate) do |topic, partition, brokers|
            racks = brokers.map { |broker| generate_broker_metadata(broker).rack.get }
            expect(racks.uniq.size).to eql(3)
          end
        end

        it 'raises exception if broker is missing rack configuration' do
          broker_metadata = generate_broker_metadata(203)
          allow(broker_metadata.rack).to receive(:defined?).and_return(false)
          expect { plan.generate }.to raise_error /Broker 203 is missing rack information/
        end

        it 'distributes each topic evenly' do
          result = Hash.new { |hash, key| hash[key] = Hash.new(0) }
          plan.generate.foreach do |tuple|
            key = tuple.first
            value = tuple.last
            ScalaEnumerable.new(value).to_a.each do |broker|
              result[key.topic][broker] += 1
            end
          end
          result.each do |topic, broker_counts|
            expect(broker_counts.keys.length).to eql(brokers.length)
            expect(broker_counts.values.max).to eql(2)
          end
        end

        context 'with existing allocations' do
          let :assignments do
            {
              'topic1' => [[101, 102, 103], [102, 103, 101], [103, 101, 102], [101, 102, 103], [102, 103, 101], [103, 101, 102]],
            }
          end

          it 'only moves overflowing partitions' do
            each_reassignment(plan.generate) do |topic, partition, brokers|
              expect(partition).to be_between(3, 5)
              expect(brokers).to match_array([201, 202, 203])
            end
          end

          it 'cycles leadership across racks' do
            rack_leaders = {}
            each_reassignment(plan.generate) do |topic, partition, brokers|
              rack_leaders[partition] = brokers.first
            end
            expect(rack_leaders).to eql({3 => 201, 4 => 202, 5 => 203})
          end
        end
      end

      context 'with broker missing rack' do
        let :brokers do
          [101, 102, 103]
        end

        before do
          rack = Scala::Option.apply(nil.to_java)
          @brokers[101] = Kafka::Admin::BrokerMetadata.new(101, rack)
        end

        it 'raises exception' do
          expect { plan.generate }.to raise_error(RuntimeError, /Broker 101 is missing rack information/)
        end
      end
    end
  end
end
