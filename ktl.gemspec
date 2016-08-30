# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'ktl/version'


Gem::Specification.new do |s|
  s.name          = 'ktl'
  s.version       = Ktl::VERSION.dup
  s.license       = 'BSD-3-Clause'
  s.authors       = ['Burt Platform Team']
  s.email         = ['mathias@burtcorp.com']
  s.homepage      = 'http://github.com/burtcorp/ktl'
  s.summary       = %q{Management tool for Kafka clusters}
  s.description   = %q{ktl is a tool that attempts to make it easier
                      to manage Kafka clusers that host a lot of topics}
  s.files         = Dir['bin/*', 'lib/**/*.rb', 'README.md', 'LICENSE.txt']
  s.require_paths = %w[lib]
  s.bindir        = 'bin'
  s.executables   = %w[ktl]

  s.platform = 'java'

  s.add_runtime_dependency 'kafka-jars', '= 0.10.0.1.pre2'
  s.add_runtime_dependency 'thor', '~> 0', '< 1.0'
  s.add_runtime_dependency 'log4j-jars', '~> 2.0'
end
