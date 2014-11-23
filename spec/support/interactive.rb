# encoding: utf-8

module InteractiveSupport
  def interactive(input, &block)
    input.each do |s|
      allow(Readline).to receive(:readline).and_return(s)
    end
    block.call
  end
end

RSpec.configure do |config|
  config.include(InteractiveSupport)
end
