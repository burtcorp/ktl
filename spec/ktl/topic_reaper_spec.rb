# encoding: utf-8

require 'spec_helper'


module Ktl
  describe TopicReaper do
    let :reaper do
      described_class.new(kafka_client, zk_client, filter, options)
    end

    let :kafka_client do
      double(:kafka_client)
    end

    let :zk_client do
      double(:zk_client).tap do |client|
        allow(client).to receive(:raw_client).and_return(client)
      end
    end

    let :filter do
      /^topic\d$/
    end

    let :utils do
      double(:utils, delete_topic: nil)
    end

    let :sleeper do
      double(:sleeper, sleep: nil)
    end

    let :logger do
      double(:logger).as_null_object
    end

    let :options do
      {
        parallel: 1,
        logger: logger,
        utils: utils,
        sleeper: sleeper,
      }
    end

    let :topics do
      {
        'topic1' => inactive_partitions(2),
        'topic2' => inactive_partitions(2),
        'topic-2' => active_partitions(3),
      }
    end

    def inactive_partitions(num_partitions)
      num_partitions.times.reduce({}) do |hash, i|
        hash.merge(i => {earliest: i, latest: i})
      end
    end

    def active_partitions(num_partitions)
      num_partitions.times.reduce({}) do |hash, i|
        hash.merge(i => {earliest: i, latest: i + 1})
      end
    end

    before do
      allow(kafka_client).to receive(:partitions) do |filter|
        topics.each_with_object({}) do |(topic, partitions), hash|
          hash[topic] = partitions.keys
        end
      end
      allow(kafka_client).to receive(:earliest_offset) do |parts|
        parts.each_with_object({}) do |(topic, partitions), hash|
          hash[topic] = partitions.each_with_object({}) do |partition, h|
            h[partition] = topics[topic][partition][:earliest]
          end
        end
      end
      allow(kafka_client).to receive(:latest_offset) do |parts|
        parts.each_with_object({}) do |(topic, partitions), hash|
          hash[topic] = partitions.each_with_object({}) do |partition, h|
            h[partition] = topics[topic][partition][:latest]
          end
        end
      end
      allow(utils).to receive(:delete_topic)
    end

    describe '#execute' do
      it 'deletes empty topics that matches the specified filter' do
        reaper.execute
        expect(utils).to have_received(:delete_topic).with(zk_client, 'topic1')
        expect(utils).to have_received(:delete_topic).with(zk_client, 'topic2')
        expect(utils).to_not have_received(:delete_topic).with(anything, 'topic-2')
      end

      it 'waits after each batch of deletes' do
        actions = []
        allow(utils).to receive(:delete_topic) do
          actions << :delete
        end
        allow(sleeper).to receive(:sleep) do
          actions << :sleep
        end
        reaper.execute
        expect(actions).to eq([:delete, :sleep, :delete, :sleep])
      end

      context 'when the specified delay is <= 0' do
        let :options do
          super.merge(delay: -1)
        end

        it 'does not wait between deletes' do
          reaper.execute
          expect(sleeper).to_not have_received(:sleep)
        end
      end
    end
  end
end
