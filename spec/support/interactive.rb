# encoding: utf-8

module InteractiveSupport
  def interactive(input, &block)
    actual_stdin = $stdin
    new_stdin = Tempfile.new('ktl-stdin')
    new_stdin.puts(input.shift) until input.empty?
    new_stdin.rewind
    $stdin.reopen(new_stdin)
    block.call
  ensure
    $stdin.reopen(actual_stdin)
    new_stdin.close
    new_stdin.unlink
  end
end

RSpec.configure do |config|
  config.include(InteractiveSupport)
end
