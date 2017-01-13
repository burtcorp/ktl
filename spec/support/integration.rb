# encoding: utf-8

shared_context 'integration setup' do
  let :zk_server do
    Kafka::Test.create_zk_server(zk_uri)
  end

  let :zk_uri do
    '127.0.0.1:2185'
  end

  let :zk_chroot do
    '/ktl-test'
  end

  let :ktl_zk do
    Kafka::Utils::ZkUtils.apply(zk_uri + zk_chroot, 30_000, 30_000, false)
  end

  let :zk_utils do
    Kafka::Utils::ZkUtils.apply(zk_uri, 30000, 30000, false)
  end

  let :zk_args do
    ['-z', zk_uri + zk_chroot]
  end

  def run(command, argv)
    Ktl::Cli.start([command, argv].flatten)
  end

  def fetch_json(path, key=nil)
    d = ktl_zk.read_data(path).first
    d = JSON.parse(d)
    key ? d[key] : d
  end

  def register_broker(id, name='localhost')
    endpoint_tuple = Scala::Tuple.new(
      Kafka::Protocol::SecurityProtocol.for_name('PLAINTEXT'),
      Kafka::Cluster::EndPoint.create_end_point("PLAINTEXT://#{name}:9092")
    )
    endpoints = Scala::Collection::Map.empty
    endpoints += endpoint_tuple
    ktl_zk.register_broker_in_zk(id, name, 9092, endpoints, 57476, Scala::Option["rack#{id}"], Kafka::Api::ApiVersion.apply('0.10.0.1'))
  end

  def clear_zk_chroot
    zk_utils.delete_path_recursive(zk_chroot)
  end

  def setup_zk_chroot
    clear_zk_chroot
    zk_utils.create_persistent_path(zk_chroot, '', no_acl)
    ktl_zk.setup_common_paths
  end

  def create_topic(*args)
    silence { run(%w[topic create], args + zk_args) }
  end

  def no_acl
    Kafka::Utils::ZkUtils::DefaultAcls(false)
  end

  def create_partitions(topic, options={})
    partitions_path = ktl_zk.class.get_topic_partitions_path(topic)
    ktl_zk.create_persistent_path(partitions_path, '', no_acl)
    partitions = options.fetch(:partitions, 1)
    partitions.times.map do |i|
      state_path = %(#{partitions_path}/#{i}/state)
      isr = options.fetch(:isr, [0])
      state = {controller_epoch: 1, leader: isr.first, leader_epoch: 1, version: 1, isr: isr}
      ktl_zk.create_persistent_path(state_path, state.to_json, no_acl)
    end
  end

  def wait_until_topics_exist(broker, topics)
    topics_exist, attempts = false, 0
    host, port = broker.split(':')
    port = port.to_i
    consumer = Kafka::Consumer::SimpleConsumer.new(host, port, 30_000, 64*1024, "ktl-integration-#{rand(100000)}")
    until topics_exist do
      request = Kafka::JavaApi::TopicMetadataRequest.new([])
      metadata = Kafka::TopicMetadataResponse.new(consumer.send(request))

      if topics.all? { |topic| metadata.leader_for(topic, 0) rescue false }
        topics_exist = true
      elsif attempts > 10
        fail('Topics not created within 10 attempts')
      else
        sleep(1)
        attempts += 1
      end
      consumer.close
      Kafka::Metrics::KafkaMetricsGroup.remove_all_consumer_metrics(consumer.client_id)
    end
  end

  before do
    zk_server.start
    setup_zk_chroot
  end

  after do
    clear_zk_chroot
    zk_utils.close
    ktl_zk.close
    zk_server.shutdown
  end
end
