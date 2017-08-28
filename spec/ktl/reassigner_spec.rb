# encoding: utf-8

require 'spec_helper'


module Ktl
  describe Reassigner do
    let :reassigner do
      described_class.new(zk_client, options)
    end

    let :zk_client do
      double(:zk_client, replica_assignment_for_topics: Scala::Collection::Map.empty)
    end

    let :zk_utils do
      Kafka::Utils::ZkUtils.new(nil, nil, false)
    end

    let :options do
      {}
    end

    describe '#reassignment_in_progress?' do
      before do
        allow(zk_client).to receive(:partitions_being_reassigned).and_return(reassignment)
      end

      context 'if there\'s a reassignment in progress' do
        let :reassignment do
          r = Scala::Collection::Map.empty
          2.times.each do |partition|
            topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
            replicas = scala_int_list([0, 1])
            r += Scala::Tuple.new(topic_partition, replicas)
          end
          r
        end

        it 'returns true' do
          expect(reassigner.reassignment_in_progress?).to eq(true)
        end
      end

      context 'if there\'s not a reassignment in progress' do
        let :reassignment do
          Scala::Collection::Map.empty
        end

        it 'returns false' do
          expect(reassigner.reassignment_in_progress?).to eq(false)
        end
      end
    end

    describe '#overflow?' do
      let :znodes do
        []
      end

      before do
        allow(zk_client).to receive(:get_children).and_return(scala_list(znodes))
      end

      context 'when there are no overflow znodes' do
        it 'returns false' do
          expect(reassigner).to_not be_overflow
        end
      end

      context 'when there are overflow znodes' do
        let :znodes do
          %w[0 1 2]
        end

        it 'returns true' do
          expect(reassigner).to be_overflow
        end
      end

      context 'when ZkNoNodeException is raised' do
        before do
          allow(zk_client).to receive(:get_children).and_raise(ZkClient::Exception::ZkNoNodeException.new)
        end

        it 'returns false' do
          expect(reassigner).to_not be_overflow
        end
      end
    end

    describe '#load_overflow' do
      let :overflow_part_1 do
        r = Scala::Collection::Map.empty
        2.times.each do |partition|
          topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
          replicas = scala_int_list([0, 1, 2])
          r += Scala::Tuple.new(topic_partition, replicas)
        end
        zk_utils.format_as_reassignment_json(r)
      end

      let :overflow_part_2 do
        r = Scala::Collection::Map.empty
        2.times.each do |partition|
          topic_partition = Kafka::TopicAndPartition.new('topic2', partition)
          replicas = scala_int_list([0, 1, 2])
          r += Scala::Tuple.new(topic_partition, replicas)
        end
        zk_utils.format_as_reassignment_json(r)
      end

      before do
        allow(zk_client).to receive(:exists?).with('/ktl/overflow').and_return(true)
        allow(zk_client).to receive(:get_children).with('/ktl/overflow').and_return(scala_list(%w[0 1]))
        allow(zk_client).to receive(:read_data).with('/ktl/overflow/0').and_return([overflow_part_1])
        allow(zk_client).to receive(:read_data).with('/ktl/overflow/1').and_return([overflow_part_2])
        allow(zk_client).to receive(:delete_znode)
      end

      it 'reads overflow from ZK' do
        overflow = reassigner.load_overflow
        expect(overflow.size).to eq(4)
      end
    end

    describe '#execute' do
      let :reassignments do
        []
      end

      let :overflow_znodes do
        []
      end

      let :reassign_znodes do
        []
      end

      before do
        allow(zk_client).to receive(:reassign_partitions) do |r|
          reassignments << JSON.parse(r).fetch('partitions')
        end
        allow(zk_client).to receive(:create_znode) do |path, data|
          if path =~ /\/ktl\/overflow\/.+/
            overflow_znodes << [path, data]
          elsif path == '/ktl/reassign'
            reassign_znodes << [path, data]
          else
            raise %(Unexpected ZooKeeper path: #{path} (#{data}))
          end
        end
        allow(zk_client).to receive(:delete_znode)
        allow(zk_client).to receive(:get_children).with('/ktl/overflow').and_return(scala_list([0, 1]))
        allow(zk_client).to receive(:exists?).with('/ktl/overflow').and_return(true)
        allow(zk_client).to receive(:exists?).with('/ktl/reassign').and_return(true)
      end

      context 'when the reassignment is less than 1MB' do
        let :reassignment do
          r = Scala::Collection::Map.empty
          10.times.each do |partition|
            topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
            replicas = scala_int_list([0, 1, 2])
            r += Scala::Tuple.new(topic_partition, replicas)
          end
          r
        end

        let :json do
          zk_utils.format_as_reassignment_json(reassignment)
        end

        it 'does not split the reassignment' do
          reassigner.execute(reassignment)
          expect(zk_client).to have_received(:reassign_partitions).with(json)
        end

        it 'removes previous overflow znodes' do
          reassigner.execute(reassignment)
          expect(zk_client).to have_received(:get_children).with('/ktl/overflow')
          2.times do |index|
            expect(zk_client).to have_received(:delete_znode).with(%(/ktl/overflow/#{index}))
          end
        end

        it 'writes the same JSON to a state path' do
          reassigner.execute(reassignment)
          expect(zk_client).to have_received(:create_znode).with('/ktl/reassign', json)
        end

        it 'removes previous state znodes' do
          reassigner.execute(reassignment)
          expect(zk_client).to have_received(:exists?).with('/ktl/reassign')
          expect(zk_client).to have_received(:delete_znode).with('/ktl/reassign')
        end
      end

      context 'when the reassignment is greater than 1MB' do
        let :reassignment do
          r = Scala::Collection::Map.empty
          20_000.times.each do |partition|
            topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
            replicas = scala_int_list([0, 1, 2])
            r += Scala::Tuple.new(topic_partition, replicas)
          end
          r
        end

        before do
          reassigner.execute(reassignment)
        end

        it 'removes previous overflow znodes if they exist' do
          expect(zk_client).to have_received(:get_children)
          expect(zk_client).to have_received(:delete_znode).with(/\/ktl\/overflow/).at_least(:once)
        end

        it 'reassigns partitions' do
          expect(zk_client).to have_received(:reassign_partitions)
        end

        it 'writes the remaining JSON to an overflow path in ZK' do
          overflow = Scala::Collection::Map.empty
          overflow = overflow_znodes.reduce(overflow) do |acc, (path, data)|
            data = zk_utils.parse_partition_reassignment_data(data)
            acc.send('++', data)
          end
          expect(overflow.size).to eq(10_000)
        end

        it 'writes the same JSON to a state path' do
          state = Scala::Collection::Map.empty
          state = reassign_znodes.reduce(state) do |acc, (path, data)|
            data = zk_utils.parse_partition_reassignment_data(data)
            acc.send('++', data)
          end
          expect(state.size).to eq(10_000)
        end
      end

      context 'with a limit' do
        let :options do
          {limit: 20}
        end

        let :reassignment do
          r = Scala::Collection::Map.empty
          100.times.each do |partition|
            topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
            replicas = scala_int_list([0, 1, 2])
            r += Scala::Tuple.new(topic_partition, replicas)
          end
          r
        end

        before do
          reassigner.execute(reassignment)
        end

        it 'splits the reassignment' do
          expect(reassignments.first.size).to eq(20)
        end

        it 'writes the remaining JSON to an overflow path in ZK' do
          overflow = Scala::Collection::Map.empty
          overflow = overflow_znodes.reduce(overflow) do |acc, (path, data)|
            data = zk_utils.parse_partition_reassignment_data(data)
            acc.send('++', data)
          end
          expect(overflow.size).to eq(80)
        end

        it 'writes the same JSON to a state path' do
          reassigned = Scala::Collection::Map.empty
          reassigned = reassign_znodes.reduce(reassigned) do |acc, (path, data)|
            data = zk_utils.parse_partition_reassignment_data(data)
            acc.send('++', data)
          end
          expect(reassigned.size).to eq(20)
        end
      end

      context 'with multi step assignment' do
        let :options do
          {
            multi_step_migration: true
          }
        end

        let :reassignment do
          r = Scala::Collection::Map.empty
          10.times.each do |partition|
            topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
            replicas = scala_int_list([0, 2])
            r += Scala::Tuple.new(topic_partition, replicas)
          end
          r
        end

        let :current_assignment do
          r = Scala::Collection::Map.empty
          10.times.each do |partition|
            topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
            replicas = scala_int_list([0, 1])
            r += Scala::Tuple.new(topic_partition, replicas)
          end
          r
        end

        let :duplicated_reassignment do
          r = Scala::Collection::Map.empty
          10.times.each do |partition|
            topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
            replicas = scala_int_list([0, 1, 2])
            r += Scala::Tuple.new(topic_partition, replicas)
          end
          r
        end

        before do
          allow(zk_client).to receive(:replica_assignment_for_topics).and_return(current_assignment)
        end

        let :final_json do
          zk_utils.format_as_reassignment_json(reassignment)
        end

        let :duplicated_json do
          zk_utils.format_as_reassignment_json(duplicated_reassignment)
        end

        it 'creates a reassignment that duplicates partitions that are being reassigned' do
          reassigner.execute(reassignment)
          expect(zk_client).to have_received(:reassign_partitions).with(duplicated_json)
        end

        it 'creates an overflow that removes the reassigned broker from the duplicated partitions' do
          reassigner.execute(reassignment)
          expect(zk_client).to have_received(:create_znode).with('/ktl/overflow/0', final_json)
        end
      end
    end
  end
end
