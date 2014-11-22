# encoding: utf-8

require 'thor'
require 'ext/kafka'

module Ktl
  KtlError = Class.new(StandardError)
  InsufficientBrokersRemainingError = Class.new(KtlError)

  module JavaConcurrent
    include_package 'java.util.concurrent'
  end
end

class Thor
  module Shell
    class Basic

      protected

      def unix?
        RUBY_PLATFORM =~ /java/i
      end
    end
  end
end

require 'ktl/command'
require 'ktl/broker'
require 'ktl/balance_plan'
require 'ktl/cluster'
require 'ktl/consumer'
require 'ktl/decommission_plan'
require 'ktl/migration_plan'
require 'ktl/reassigner'
require 'ktl/topic'
require 'ktl/cli'
require 'ktl/zookeeper_client'
