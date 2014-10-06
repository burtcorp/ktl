# encoding: utf-8

require 'thor'
require 'ext/kafka'

module Ktl
  module JavaConcurrent
    include_package 'java.util.concurrent'
  end

  class ScalaEnumerable
    include Enumerable

    def initialize(underlying)
      @underlying = underlying
    end

    def each(&block)
      @underlying.foreach(&block)
    end
  end
end

require 'ktl/command'
require 'ktl/broker'
require 'ktl/balance_plan'
require 'ktl/cluster'
require 'ktl/consumer'
require 'ktl/migration_plan'
require 'ktl/topic'
require 'ktl/cli'
require 'ktl/zookeeper_client'
