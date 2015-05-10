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

require 'support/cli_helpers'
require 'support/interactive'
require 'support/scala_helpers'
require 'support/integration'
require 'support/kafka_utils'
require 'support/kafka_ctl'
