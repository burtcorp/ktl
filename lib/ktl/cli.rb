# encoding: utf-8

module Ktl
  class Cli < Thor
    desc 'broker SUBCOMMAND ...ARGS', 'commands for managing brokers'
    subcommand 'broker', Broker

    desc 'topic SUBCOMMAND ...ARGS', 'commands for managing topics'
    subcommand 'topic', Topic
  end
end
