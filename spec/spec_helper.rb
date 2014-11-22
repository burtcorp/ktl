# encoding: utf-8

require 'bundler/setup'
require 'json'

require 'simplecov'

SimpleCov.start do
  add_group 'Source', 'lib'
  add_group 'Unit tests', 'spec/ktl'
  add_group 'Integration tests', 'spec/integration'
  add_group 'Support', 'spec/support'
end

require 'ktl'

require 'support/cli_helpers'
require 'support/fake_set'
require 'support/scala_helpers'

RSpec.configure do |config|
  config.include(CliHelpers)
  config.include(ScalaHelpers)
end
