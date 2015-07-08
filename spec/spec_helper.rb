# encoding: utf-8

require 'tempfile'
require 'simplecov'

SimpleCov.start do
  add_group 'Source', 'lib'
  add_group 'Unit tests', 'spec/ktl'
  add_group 'Integration tests', 'spec/integration'
  add_group 'Support', 'spec/support'
end

require 'ktl'

RSpec.configure do |config|
  config.order = 'random'
  config.raise_errors_for_deprecations!
  config.before(:suite) do
    if ENV['SILENCE_LOGGING'] == 'no'
      Log4j::BasicConfigurator.reset_configuration
      Log4j::Logger.root_logger.set_level(Log4j::Level::INFO)
      log_file = File.expand_path('../../tmp/kafka.log', __FILE__)
      FileUtils.mkdir_p(File.dirname(log_file))
      layout = Log4j::PatternLayout.new('[%d] %p %m (%c)%n')
      appender = Log4j::FileAppender.new(layout, log_file, append = true)
      Log4j::BasicConfigurator.configure(appender)
    else
      Log4j::Logger.root_logger.set_level(Log4j::Level::FATAL)
    end
  end
end

require 'support/cli_helpers'
require 'support/interactive'
require 'support/scala_helpers'
require 'support/integration'
require 'support/kafka_utils'
