# encoding: utf-8

require 'bundler/setup'
require 'json'
require 'ktl'

require 'support/cli_helpers'
require 'support/fake_set'
require 'support/scala_helpers'

RSpec.configure do |config|
  config.include(CliHelpers)
  config.include(ScalaHelpers)
end
