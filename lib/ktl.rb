# encoding: utf-8

require 'thor'
require 'json'
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
require 'ktl/broker'
require 'ktl/cluster'
require 'ktl/consumer'
require 'ktl/decommission_plan'
require 'ktl/migration_plan'
require 'ktl/reassigner'
require 'ktl/reassignment_progress'
require 'ktl/reassignment_task'
require 'ktl/shuffle_plan'
require 'ktl/topic'
require 'ktl/cli'
require 'ktl/zookeeper_client'
