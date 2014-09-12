# encoding: utf-8

module Ktl
  class Cli < Thor
    desc 'topic SUBCOMMAND ...ARGS', 'commands for managing topics'
    subcommand 'topic', Topic
  end
end
