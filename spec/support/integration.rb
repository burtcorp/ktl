# encoding: utf-8

shared_context 'integration setup' do
  let :zk_server do
    Kafka::Test.create_zk_server(zk_uri)
  end

  let :zk_uri do
    'localhost:2185'
  end

  let :control_zk do
    Kafka::Utils.new_zk_client(zk_uri)
  end

  let :ktl_zk do
    Kafka::Utils.new_zk_client(zk_uri + '/ktl-test')
  end

  let :zk_args do
    ['-z', zk_uri + '/ktl-test']
  end

  def run(command, argv)
    Ktl::Cli.start([command, argv].flatten)
  end

  def fetch_json(path, key=nil)
    d = Kafka::Utils::ZkUtils.read_data(ktl_zk, path).first
    d = JSON.parse(d)
    key ? d[key] : d
  end

  def register_broker(id, name='localhost')
    Kafka::Utils::ZkUtils.register_broker_in_zk(ktl_zk, id, name, 9092, 1, 57476)
  end

  def clear_zk_chroot
    Kafka::Utils::ZkUtils.delete_path_recursive(control_zk, '/ktl-test')
  end

  def setup_zk_chroot
    clear_zk_chroot
    Kafka::Utils::ZkUtils.make_sure_persistent_path_exists(control_zk, '/ktl-test')
    Kafka::Utils::ZkUtils.setup_common_paths(ktl_zk)
  end

  def create_topic(*args)
    silence { run(%w[topic create], args + zk_args) }
  end

  def create_partitions(topic, options={})
    partitions_path = Kafka::Utils::ZkUtils.get_topic_partitions_path(topic)
    Kafka::Utils::ZkUtils.create_persistent_path(ktl_zk, partitions_path, '')
    partitions = options.fetch(:partitions, 1)
    partitions.times.map do |i|
      state_path = %(#{partitions_path}/#{i}/state)
      isr = options.fetch(:isr, [0])
      state = {controller_epoch: 1, leader: isr.first, leader_epoch: 1, version: 1, isr: isr}
      Kafka::Utils::ZkUtils.create_persistent_path(ktl_zk, state_path, state.to_json)
    end
  end

  def wait_until_topics_exist(broker, topics)
    topics_exist, attempts = false, 0
    until topics_exist do
      consumer = Heller::Consumer.new(broker)
      metadata = consumer.metadata
      if topics.all? { |topic| metadata.leader_for(topic, 0) rescue false }
        topics_exist = true
      elsif attempts > 10
        fail('Topics not created within 10 attempts')
      else
        sleep(1)
        attempts += 1
      end
      consumer.close
    end
  end

  before do
    zk_server.start
    setup_zk_chroot
  end

  after do
    clear_zk_chroot
    control_zk.close
    ktl_zk.close
    zk_server.shutdown
  end
end
