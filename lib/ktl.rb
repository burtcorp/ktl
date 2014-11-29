# encoding: utf-8

require 'thor'
require 'ext/kafka'
require 'ext/thor'

module Ktl
  KtlError = Class.new(StandardError)
  InsufficientBrokersRemainingError = Class.new(KtlError)

  CanBuildFrom = Scala::Collection::Immutable::List.can_build_from

  module JavaConcurrent
    include_package 'java.util.concurrent'
  end
end

require 'ktl/command'
require 'ktl/control'
require 'ktl/broker'
require 'ktl/balance_plan'
require 'ktl/cluster'
require 'ktl/consumer'
require 'ktl/decommission_plan'
require 'ktl/migration_plan'
require 'ktl/reassigner'
require 'ktl/reassignment_progress'
require 'ktl/topic'
require 'ktl/cli'
require 'ktl/zookeeper_client'
