# encoding: utf-8

require 'bundler/setup'
require 'json'
require 'ktl'

require 'support/cli_helpers'
require 'support/fake_set'

RSpec.configure do |config|
  config.include(CliHelpers)
end
