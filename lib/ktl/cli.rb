# encoding: utf-8

module Ktl
  class Cli < Thor
    desc 'consumer SUBCOMMAND ...ARGS', 'Commands for managing consumers'
    subcommand 'consumer', Consumer

    desc 'cluster SUBCOMMAND ...ARGS', 'Commands for managing a cluster'
    subcommand 'cluster', Cluster

    desc 'topic SUBCOMMAND ...ARGS', 'Commands for managing topics'
    subcommand 'topic', Topic
  end
end
