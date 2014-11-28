# encoding: utf-8

require 'spec_helper'


module Ktl
  describe Reassigner do
    let :reassigner do
      described_class.new(:type, zk_client, options)
    end

    let :zk_client do
      double(:zk_client)
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
          2.times.map do |partition|
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
        allow(zk_client).to receive(:get_children).and_return(znodes)
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
        2.times.map do |partition|
          topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
          replicas = scala_int_list([0, 1, 2])
          r += Scala::Tuple.new(topic_partition, replicas)
        end
        Kafka::Utils::ZkUtils.get_partition_reassignment_zk_data(r)
      end

      let :overflow_part_2 do
        r = Scala::Collection::Map.empty
        2.times.map do |partition|
          topic_partition = Kafka::TopicAndPartition.new('topic2', partition)
          replicas = scala_int_list([0, 1, 2])
          r += Scala::Tuple.new(topic_partition, replicas)
        end
        Kafka::Utils::ZkUtils.get_partition_reassignment_zk_data(r)
      end

      before do
        allow(zk_client).to receive(:get_children).with('/ktl/overflow/type').and_return(scala_list(%w[0 1]))
        allow(zk_client).to receive(:read_data).with('/ktl/overflow/type/0').and_return([overflow_part_1])
        allow(zk_client).to receive(:read_data).with('/ktl/overflow/type/1').and_return([overflow_part_2])
      end

      it 'reads overflow from ZK' do
        overflow = reassigner.load_overflow
        expect(overflow.size).to eq(4)
      end
    end

    describe '#execute' do
      before do
        allow(zk_client).to receive(:reassign_partitions)
        allow(zk_client).to receive(:create_znode).with(/\/ktl\/reassign\/type/, anything)
      end

      context 'when the reassignment is less than `json_max_limit`' do
        let :reassignment do
          r = Scala::Collection::Map.empty
          10.times.map do |partition|
            topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
            replicas = scala_int_list([0, 1, 2])
            r += Scala::Tuple.new(topic_partition, replicas)
          end
          r
        end

        let :json do
          Kafka::Utils::ZkUtils.get_partition_reassignment_zk_data(reassignment)
        end

        before do
          allow(zk_client).to receive(:delete_znode)
        end

        it 'does not split the reassignment' do
          reassigner.execute(reassignment)
          expect(zk_client).to have_received(:reassign_partitions).with(json)
        end

        it 'unconditionally removes previous overflow znodes' do
          reassigner.execute(reassignment)
          expect(zk_client).to have_received(:delete_znode).with('/ktl/overflow/type', recursive: true)
        end

        it 'writes the same json to a state path' do
          reassigner.execute(reassignment)
          expect(zk_client).to have_received(:create_znode).with('/ktl/reassign/type/0', json)
        end

        it 'unconditionally removes previous state znodes' do
          reassigner.execute(reassignment)
          expect(zk_client).to have_received(:delete_znode).with('/ktl/reassign/type', recursive: true)
        end
      end

      context 'when the reassignment is greater than `json_max_limit`' do
        let :reassignment do
          r = Scala::Collection::Map.empty
          10.times.map do |partition|
            topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
            replicas = scala_int_list([0, 1, 2])
            r += Scala::Tuple.new(topic_partition, replicas)
          end
          r
        end

        let :scaled_down_json do
          r = Scala::Collection::Map.empty
          [3, 4, 9].each do |partition|
            topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
            replicas = scala_int_list([0, 1, 2])
            r += Scala::Tuple.new(topic_partition, replicas)
          end
          r
          Kafka::Utils::ZkUtils.get_partition_reassignment_zk_data(r)
        end

        let :options do
          {json_max_size: 150}
        end

        let :overflow_znodes do
          []
        end

        let :reassign_znodes do
          []
        end

        before do
          allow(zk_client).to receive(:create_znode) do |path, data|
            if path =~ /\/ktl\/overflow\/.+/
              overflow_znodes << [path, data]
            elsif path =~ /\/ktl\/reassign\/.+/
              reassign_znodes << [path, data]
            end
          end
          allow(zk_client).to receive(:delete_znode).with('/ktl/reassign/type', recursive: true)
          reassigner.execute(reassignment)
        end

        it 'splits the reassignment' do
          expect(zk_client).to have_received(:reassign_partitions).with(scaled_down_json)
        end

        it 'writes the remaining json to ZK' do
          overflow = Scala::Collection::Map.empty
          overflow = overflow_znodes.reduce(overflow) do |acc, (path, data)|
            data = Kafka::Utils::ZkUtils.parse_partition_reassignment_data(data)
            acc.send('++', data)
          end
          expect(overflow.size).to eq(7)
        end

        it 'writes the same json to a state path, splitted' do
          reassign = Scala::Collection::Map.empty
          reassign = reassign_znodes.reduce(reassign) do |acc, (path, data)|
            data = Kafka::Utils::ZkUtils.parse_partition_reassignment_data(data)
            acc.send('++', data)
          end
          expect(reassign.size).to eq(10)
        end
      end
    end
  end
end
