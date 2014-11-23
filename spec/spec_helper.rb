# encoding: utf-8

require 'bundler/setup'
require 'json'
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
require 'support/fake_set'
require 'support/interactive'
require 'support/scala_helpers'
require 'support/integration'
