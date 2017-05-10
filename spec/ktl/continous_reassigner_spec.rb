# encoding: utf-8

require 'spec_helper'


module Ktl
  describe ContinousReassigner do
    let :reassigner do
      described_class.new(zk_client, options)
    end

    let :zk_client do
      double(:zk_client, replica_assignment_for_topics: Scala::Collection::Map.empty)
    end

    let :options do
      {
        delay: 0.1,
        log_assignments: true,
        sleeper: sleeper,
      }
    end

    let :sleeper do
      double(:sleeper, sleep: nil)
    end

    describe '#execute' do
      let :reassignments do
        []
      end

      let :reassignment do
        r = Scala::Collection::Map.empty
        10.times.map do |partition|
          topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
          replicas = scala_int_list([0, 1, 2])
          r += Scala::Tuple.new(topic_partition, replicas)
        end
        r
      end

      let :zk_utils do
        Kafka::Utils::ZkUtils.new(nil, nil, false)
      end

      let :json do
        zk_utils.format_as_reassignment_json(reassignment)
      end

      let :znodes do
        {}
      end

      before do
        allow(zk_client).to receive(:reassign_partitions) do |r|
          reassignments << JSON.parse(r).fetch('partitions')
        end
        allow(zk_client).to receive(:create_znode) do |path, data|
          znodes[path] = data
        end
        allow(zk_client).to receive(:delete_znode) do |path|
          znodes.delete(path)
        end
        allow(zk_client).to receive(:get_children) do |path|
          matching = znodes.select { |k, v| k.start_with?(path) }
          matching = matching.keys.map { |s| s.split('/').last }
          scala_list(matching)
        end
        allow(zk_client).to receive(:exists?) do |path|
          znodes.key?(path)
        end
        allow(zk_client).to receive(:read_data) do |path|
          [znodes[path]]
        end
        allow(zk_client).to receive(:watch_data)
        allow(zk_client).to receive(:unsubscribe_data)
        allow(zk_client).to receive(:partitions_being_reassigned).and_return([])
      end

      def reassign
        latch = JavaConcurrent::CountDownLatch.new(1)
        t = Thread.new { latch.count_down; reassigner.execute(reassignment) }
        latch.await
        if block_given?
          yield t
        else
          reassigner.handle_data_deleted(Kafka::Utils::ZkUtils.reassign_partitions_path)
        end
        t.join
        expect(t.status).to eq(false)
      end

      it 'reassigns partitions' do
        reassign
        expect(zk_client).to have_received(:reassign_partitions).with(json)
      end

      it 'watches the reassignment path for data changes' do
        reassign
        expect(zk_client).to have_received(:watch_data).with(Kafka::Utils::ZkUtils.reassign_partitions_path, reassigner)
      end

      it 'removes the watcher when the reassignment is done' do
        reassign
        expect(zk_client).to have_received(:unsubscribe_data).with(Kafka::Utils::ZkUtils.reassign_partitions_path, reassigner)
      end

      context 'when the reassignment is greater than 1MB' do
        let :reassignment do
          r = Scala::Collection::Map.empty
          25_000.times.each do |partition|
            topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
            replicas = scala_int_list([0, 1, 2])
            r += Scala::Tuple.new(topic_partition, replicas)
          end
          r
        end

        it 'splits each reassignment' do
          reassign do
            sleep 0.1 until znodes.any? { |k, _| k == '/ktl/reassign' }
            overflow = znodes.select { |k, v| k.start_with?('/ktl/overflow') }
            expect(overflow).to_not be_empty
            while znodes.any? { |k, _| k == '/ktl/reassign' }
              reassigner.handle_data_deleted(Kafka::Utils::ZkUtils.reassign_partitions_path)
            end
          end
        end

        it 'eventually reassigns all partitions' do
          reassign do
            sleep 0.1 until znodes.any? { |k, _| k == '/ktl/reassign' }
            while znodes.any? { |k, _| k == '/ktl/reassign' }
              reassigner.handle_data_deleted(Kafka::Utils::ZkUtils.reassign_partitions_path)
            end
          end
          count = reassignments.reduce(0) { |acc, r| acc + r.size }
          expect(count).to eq(25_000)
        end
      end

      context 'with a limit' do
        let :options do
          {limit: 2}
        end

        it 'splits the reassignment' do
          reassign do
            sleep 0.1 until znodes.any? { |k, _| k == '/ktl/reassign' }
            overflow = znodes.select { |k, v| k.start_with?('/ktl/overflow') }
            expect(overflow).to_not be_empty
            while znodes.any? { |k, _| k == '/ktl/reassign' }
              reassigner.handle_data_deleted(Kafka::Utils::ZkUtils.reassign_partitions_path)
            end
          end
        end

        it 'eventually reassigns all partitions' do
          reassign do
            sleep 0.1 until znodes.any? { |k, _| k == '/ktl/reassign' }
            while znodes.any? { |k, _| k == '/ktl/reassign' }
              reassigner.handle_data_deleted(Kafka::Utils::ZkUtils.reassign_partitions_path)
            end
          end
          count = reassignments.reduce(0) { |acc, r| acc + r.size }
          expect(count).to eq(10)
        end
      end
    end
  end
end
