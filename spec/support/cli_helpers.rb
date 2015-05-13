# encoding: utf-8

module CliHelpers
  java_import 'java.io.ByteArrayOutputStream'
  java_import 'scala.Console'

  def capture(stream=:stdout, &block)
    s = nil
    begin
      stderr, stdout, $stderr, $stdout = $stderr, $stdout, StringIO.new, StringIO.new
      result = (stream == :stdout) ? $stdout : $stderr
      result.puts(capture_scala(stream, &block))
      s = result.string
    ensure
      $stderr, $stdout = stderr, stdout
    end
    puts s if ENV['SILENCE_LOGGING'] == 'no' && !s.empty?
    s
  end
  alias_method :silence, :capture

  def capture_scala(stream=:stdout, &block)
    result = ByteArrayOutputStream.new
    if stream == :stdout
      Console.with_out(result) do
        block.call
      end
    else
      Console.with_err(result) do
        block.call
      end
    end
    s = result.to_string
    puts s if ENV['SILENCE_LOGGING'] == 'no' && !s.empty?
    s
  end
  alias_method :silence_scala, :capture_scala
end

RSpec.configure do |config|
  config.include(CliHelpers)
end
