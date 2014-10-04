# encoding: utf-8

module Ktl
  class Cli < Thor
    desc 'broker SUBCOMMAND ...ARGS', 'commands for managing brokers'
    subcommand 'broker', Broker

    desc 'topic SUBCOMMAND ...ARGS', 'commands for managing topics'
    subcommand 'topic', Topic

    desc 'consumer SUBCOMMAND ...ARGS', 'commands for managing consumers'
    subcommand 'consumer', Consumer
  end
end
