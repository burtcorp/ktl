# encoding: utf-8

module Ktl
  class Command < Thor

    java_import 'java.io.ByteArrayOutputStream'

    private

    def with_zk_client
      zk_client = ZookeeperClient.new(options.zookeeper).setup
      yield zk_client
    rescue => e
      logger.error '%s (%s)' % [e.message, e.class.name]
      logger.debug e.backtrace.join($/)
      $stderr.puts '%s (%s)' % [e.message, e.class.name]
      $stderr.puts e.backtrace.join($/)
    ensure
      zk_client.close if zk_client
    end

    def logger
      @logger ||= Logger.new($stdout).tap do |log|
        log.formatter = ShellFormater.new(shell)
      end
    end

    def silence_scala(&block)
      block.call
      # baos = ByteArrayOutputStream.new
      # Scala::Console.with_out(baos) { block.call }
    end
  end
end
