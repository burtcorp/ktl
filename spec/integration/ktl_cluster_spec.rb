# encoding: utf-8

require 'spec_helper'


describe 'bin/ktl cluster' do
  include_context 'integration setup'

  before do
    register_broker(0, 'test-host-0')
    create_topic(%w[topic1 --partitions 2])
    create_partitions('topic1', partitions: 2)
  end

  describe 'stats' do
    it 'prints statistics about cluster' do
      output = capture { run(['cluster', 'stats'], zk_args) }
      expect(output).to include('Cluster status:')
      expect(output).to include('topics: 1 (2 partitions)')
      expect(output).to include('brokers: 1')
      expect(output).to include('- 0 (test-host-0) leader for 2 partitions (100.00 %)')
    end
  end
end
