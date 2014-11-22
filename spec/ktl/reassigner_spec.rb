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

    describe '#in_progress?' do
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
          expect(reassigner).to be_in_progress
        end
      end

      context 'if there\'s not a reassignment in progress' do
        let :reassignment do
          Scala::Collection::Map.empty
        end

        it 'returns false' do
          expect(reassigner).to_not be_in_progress
        end
      end
    end

    describe '#overflow?' do
      around do |example|
        Dir.mktmpdir do |dir|
          Dir.chdir(dir) do
            example.call
          end
        end
      end

      context 'if there\'s an overflow file' do
        let :json do
          r = Scala::Collection::Map.empty
          2.times.map do |partition|
            topic_partition = Kafka::TopicAndPartition.new('topic1', partition)
            replicas = scala_int_list([0, 1, 2])
            r += Scala::Tuple.new(topic_partition, replicas)
          end
          r
          Kafka::Utils::ZkUtils.get_partition_reassignment_zk_data(r)
        end

        it 'returns true' do
          File.open('.type-overflow.json', 'w+') do |file|
            file.puts(json)
          end
          expect(reassigner).to be_overflow
        end
      end

      context 'if there\'s not an overflow file' do
        it 'returns false' do
          expect(reassigner).to_not be_overflow
        end
      end
    end

    describe '#execute' do
      around do |example|
        Dir.mktmpdir do |dir|
          Dir.chdir(dir) do
            example.call
          end
        end
      end

      before do
        allow(zk_client).to receive(:reassign_partitions)
        reassigner.execute(reassignment)
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

        it 'does not split the reassignment' do
          expect(zk_client).to have_received(:reassign_partitions).with(json)
        end

        it 'does not write an overflow file' do
          expect(File.exists?('.type-overflow.json')).to be false
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

        it 'splits the reassignment' do
          expect(zk_client).to have_received(:reassign_partitions).with(scaled_down_json)
        end

        it 'writes the remaining json to disk' do
          overflow_json = File.read('.type-overflow.json')
          overflow = Kafka::Utils::ZkUtils.parse_partition_reassignment_data(overflow_json)
          expect(overflow.size).to eq(7)
        end
      end
    end
  end
end
