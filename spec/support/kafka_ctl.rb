# encoding: utf-8

module Support
  class KafkaCtl
    def initialize(options={})
      @cluster_size = options[:cluster_size] || 1
      @zookeeper_port = options[:zookeeper_port] || 2185
      @cmd_path = File.expand_path('../bin/kafkactl', __FILE__)
    end

    def start
      run('start')
    end

    def stop
      run('stop')
    end

    def clear
      run('clear')
    end

    private

    def run(subcommand)
      %x(#{env} #{@cmd_path} #{subcommand})
    end

    def env
      @env ||= begin
        env = []
        env << %(ZOOKEEPER_PORT=#{@zookeeper_port}) if @zookeeper_port
        env << %(KAFKA_BASE_PORT=#{@kafka_base_port}) if @kafka_base_port
        env << %(KAFKA_BROKER_COUNT=#{@cluster_size}) if @cluster_size
        env.join(' ')
      end
    end
  end
end
