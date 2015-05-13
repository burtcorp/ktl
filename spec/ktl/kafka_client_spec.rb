# encoding: utf-8

require 'spec_helper'


module Ktl
  describe KafkaClient do
    let :client do
      described_class.new(config)
    end

    let :config do
      {
        hosts: %w[kafka:9092 kafka:9093 kafka:9094],
        consumer_impl: consumer_impl,
        backoff: backoff,
        sleeper: sleeper,
        logger: logger,
      }
    end

    let :consumer_impl do
      double(:consumer_impl)
    end

    let :backoff do
      2
    end

    let :sleeper do
      double(:sleeper, sleep: nil)
    end

    let :logger do
      double(:logger, debug: nil, info: nil, warn: nil)
    end

    let :topic_metadata do
      double(:topic_metadata)
    end

    let :topic_name do
      'some.topic.name'
    end

    let :partition_metadata do
      [
        double(partition_id: 0),
        double(partition_id: 1),
        double(partition_id: 2),
        double(partition_id: 3),
        double(partition_id: 4),
        double(partition_id: 5),
      ]
    end

    let :brokers do
      brokers = [
        double(connection_string: 'kafka:9092'),
        double(connection_string: 'kafka:9093'),
        double(connection_string: 'kafka:9094'),
      ]
      brokers * 2
    end

    let :consumers do
      {
        'kafka:9092' => double(:consumer0),
        'kafka:9093' => double(:consumer1),
        'kafka:9094' => double(:consumer2),
      }
    end

    let :metadata_requests do
      Hash.new(0)
    end

    let :partition_errors do
      [0] * partition_metadata.size
    end

    before do
      allow(consumer_impl).to receive(:new) do |connection_string|
        consumers[connection_string]
      end
    end

    before do
      s = allow(topic_metadata).to(receive(:each))
      partition_metadata.each do |meta|
        s.and_yield(topic_name, meta)
        allow(topic_metadata).to receive(:leader_for).with(topic_name, meta.partition_id).and_return(brokers[meta.partition_id])
      end
    end

    before do
      consumers.each do |connection_string, consumer|
        allow(consumer).to receive(:offsets_before) do |*requests|
          response = double(:offset_response)
          allow(response).to receive(:error?) do
            brokers.each_with_index.any? { |_, i| partition_errors[i] != 0 }
          end
          allow(response).to receive(:error) do |_, partition_id|
            partition_errors[partition_id]
          end
          partition_offsets.each do |partition, offsets|
            if partition_errors[partition].zero?
              allow(response).to receive(:offsets).with(topic_name, partition).and_return(Array(offsets))
            end
          end
          response
        end
        allow(consumer).to receive(:metadata) do
          metadata_requests[connection_string] = metadata_requests[connection_string] + 1
          topic_metadata
        end
      end
    end

    before do
      client.setup
    end

    shared_examples 'error handling of an retryable error' do
      let :index do
        1
      end

      let :consumer do
        consumers[brokers[index].connection_string]
      end

      let :log_messages do
        []
      end

      before do
        tries = 3
        consumers.each do |connection_string, consumer|
          allow(consumer).to receive(:metadata) do
            metadata_requests[connection_string] = metadata_requests[connection_string] + 1
            if tries.zero?
              partition_errors[index] = 0
            else
              tries -= 1
            end
            topic_metadata
          end
        end
        partition_errors[index] = error_code
      end

      before do
        allow(logger).to receive(:debug) do |&block|
          log_messages << block.call
        end
      end

      before do
        client.send(method_name, {'some.topic.name' => [0, 1, 2]}, *method_args)
      end

      it 'retries error\'ed partitions' do
        expect(consumer).to have_received(:offsets_before).exactly(4).times
      end

      it 'fetches new metadata' do
        expect(metadata_requests.values.reduce(:+)).to eq(4)
      end

      it 'waits a bit before retrying' do
        expect(sleeper).to have_received(:sleep).with(backoff*1000).exactly(3).times
      end

      it 'logs a message about retrying' do
        messages = log_messages.select { |message| message.match(/Failed to find metadata/) }
        expect(messages.size).to eq(3)
      end
    end

    shared_examples 'dealing with retryable errors' do
      context 'when a NotLeaderForPartition error code is returned' do
        let :error_code do
          6
        end

        include_examples 'error handling of an retryable error'
      end

      context 'when a UnknownTopicOrPartition error code is returned' do
        let :error_code do
          3
        end

        include_examples 'error handling of an retryable error'
      end

      context 'when a LeaderNotAvailable error code is returned' do
        let :error_code do
          5
        end

        include_examples 'error handling of an retryable error'
      end
    end

    describe '#setup' do
      it 'connects to each broker' do
        consumers.each_key do |connection_string|
          expect(consumer_impl).to have_received(:new).with(connection_string)
        end
      end
    end

    describe '#partitions' do
      it 'fetches metadata from a consumer' do
        client.partitions
        expect(metadata_requests.values.reduce(:+)).to eq(1)
      end

      context 'without an explicit filter' do
        it 'returns a hash of current known topics and partitions' do
          expect(client.partitions).to eq(topic_name => partition_metadata.map(&:partition_id))
        end
      end

      context 'with an explicit filter' do
        it 'ignores other topics' do
          expect(client.partitions(/other/)).to eq({})
        end
      end
    end

    describe '#earliest_offset' do
      let :partition_offsets do
        partition_metadata.each_with_object({}) do |partition, hash|
          hash[partition.partition_id] = partition.partition_id * 2
        end
      end

      let :method_name do
        :earliest_offset
      end

      let :method_args do
        []
      end

      it 'fetches earliest offsets for each partition' do
        offsets = client.earliest_offset({'some.topic.name' => [0, 1, 2]})
        expect(offsets).to eq(topic_name => Hash[partition_offsets.take(3)])
      end

      include_examples 'dealing with retryable errors'
    end

    describe '#latest_offset' do
      let :partition_offsets do
        partition_metadata.each_with_object({}) do |partition, hash|
          hash[partition.partition_id] = (partition.partition_id * 2) + 1
        end
      end

      let :method_name do
        :latest_offset
      end

      let :method_args do
        []
      end

      it 'fetches latest offset for each partition' do
        offsets = client.latest_offset({'some.topic.name' => [0, 1, 2]})
        expect(offsets).to eq(topic_name => Hash[partition_offsets.take(3)])
      end

      include_examples 'dealing with retryable errors'
    end

    describe '#offset_before' do
      let :partition_offsets do
        partition_metadata.each_with_object({}) do |partition, hash|
          hash[partition.partition_id] = [partition.partition_id + 1]
        end
      end

      let :method_name do
        :offset_before
      end

      let :method_args do
        [Time.utc(2015, 3, 19, 13).to_i]
      end

      it 'fetches offsets before the given timestamp for each partition' do
        offsets = client.offset_before({'some.topic.name' => [0, 1, 2]}, *method_args)
        expect(offsets).to eq(topic_name => Hash[partition_offsets.take(3).map { |k, v| [k, v.first] }])
      end

      include_examples 'dealing with retryable errors'
    end
  end
end

