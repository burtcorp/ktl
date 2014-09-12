# encoding: utf-8

module CliHelpers
  java_import 'java.io.ByteArrayOutputStream'
  java_import 'scala.Console'

  def capture(stream=:stdout, &block)
    begin
      stderr, stdout, $stderr, $stdout = $stderr, $stdout, StringIO.new, StringIO.new
      result = (stream == :stdout) ? $stdout : $stderr
      result.puts(capture_scala(stream, &block))
      result.string
    ensure
      $stderr, $stdout = stderr, stdout
    end
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
    result.to_string
  end
  alias_method :silence_scala, :capture_scala
end
