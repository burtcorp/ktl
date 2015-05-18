# encoding: utf-8

module Ktl
  class Cli < Thor
    desc 'consumer SUBCOMMAND ...ARGS', 'commands for managing consumers'
    subcommand 'consumer', Consumer

    desc 'cluster SUBCOMMAND ...ARGS', 'commands for managing a cluster'
    subcommand 'cluster', Cluster

    desc 'topic SUBCOMMAND ...ARGS', 'commands for managing topics'
    subcommand 'topic', Topic
  end
end
