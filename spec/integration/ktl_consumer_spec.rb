# encoding: utf-8

require 'spec_helper'


describe 'bin/ktl consumer' do
  include_context 'integration setup'

  let :consumers_count do
    1
  end

  let :broker_port do
    9192
  end

  let :broker do
    Kafka::Test.create_kafka_server({
      'broker.id' => 0,
      'port' => broker_port,
      'zookeeper.connect' => zk_uri + zk_chroot,
      'log.flush.interval.messages' => 5,
    })
  end

  let :consumers do
    consumers_count.times.map do |index|
      Heller::ZookeeperConsumer.new(zk_uri + zk_chroot, {
        group_id: 'ktl-test-consumers',
        consumer_id: 'ktl-test-consumer-' + index.to_s,
        rebalance_retries: 20,
        rebalance_retry_backoff: 100,
        auto_reset_offset: :smallest,
        fetch_max_wait: 100,
        timeout: 1000,
        socket_timeout: 1000,
        auto_commit_interval: 500,
      })
    end
  end

  let :topics do
    2.times.map { |i| %(ktl-test-topic#{i}) }
  end

  before do
    clear_zk_chroot
    broker.start
    topics.each do |topic|
      create_topic(%W[#{topic} --partitions 8])
    end
    wait_until_topics_exist(%(localhost:#{broker_port}), topics)
    topics.each do |topic|
      publish_messages(%(localhost:#{broker_port}), topic, 20)
    end
  end

  after do
    broker.shutdown
  end

  describe 'lag' do
    let :streams do
      consumers.flat_map { |c| c.create_streams_by_filter('ktl-test-topic.*', 1) }.map(&:iterator)
    end

    let :output do
      capture { run(%w[consumer lag ktl-test-consumers], zk_args) }.split("\n")
    end

    before do
      streams.each do |stream|
        3.times { stream.next }
      end
      consumers.each(&:commit)
    end

    after do
      consumers.each(&:close)
    end

    it 'prints a table with progress information about given consumer group' do
      output.shift # get rid of header
      output.each do |line|
        line = line.split
        expect(line[0]).to eq('ktl-test-consumers')
        expect(line[1]).to satisfy { |topic| topics.include?(topic) }
        expect(line[6]).to eq('ktl-test-consumers_ktl-test-consumer-0-0')
      end
    end
  end
end
