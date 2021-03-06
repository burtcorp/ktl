# encoding: utf-8

unless defined?($SLF4J_BACKEND)
  $SLF4J_BACKEND = 'log4j12'
end

require 'thor'
require 'json'
require 'logger'
require 'ext/kafka'
require 'ext/thor'

module Ktl
  KtlError = Class.new(StandardError)
  InsufficientBrokersRemainingError = Class.new(KtlError)

  CanBuildFrom = Scala::Collection::Immutable::List.can_build_from

  module JavaConcurrent
    include_package 'java.util.concurrent'
  end

  class NullLogger
    def close(*); end
    def debug(*); end
    def debug?; false end
    def error(*); end
    def error?; false end
    def fatal(*); end
    def fatal?; false end
    def info(*); end
    def info?; false end
    def unknown(*); end
    def warn(*); end
    def warn?; false end
  end
end

require 'ktl/command'
require 'ktl/cluster'
require 'ktl/cluster_stats_task'
require 'ktl/decommission_plan'
require 'ktl/migration_plan'
require 'ktl/reassigner'
require 'ktl/continous_reassigner'
require 'ktl/reassignment_progress'
require 'ktl/reassignment_task'
require 'ktl/shuffle_plan'
require 'ktl/shell_formatter'
require 'ktl/topic'
require 'ktl/cli'
require 'ktl/zookeeper_client'
