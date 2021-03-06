# encoding: utf-8

require 'spec_helper'


module Ktl
  describe ReassignmentProgress do
    let :progress do
      described_class.new(zk_client, options.merge(logger: logger))
    end

    let :zk_client do
      double(:zk_client)
    end

    let :options do
      {}
    end

    let :shell do
      double(:shell)
    end

    let :logger do
      double(:logger, debug: nil, info: nil, warn: nil, error: nil)
    end

    before do
      allow(shell).to receive(:print_table)
    end

    describe '#display' do
      context 'when there\'s no difference between current and state' do
        let :reassignment_json do
          {
            'partitions' => [
              {'topic' => 'topic1', 'partition' => 0, 'replicas' => [0]},
            ]
          }.to_json
        end

        before do
          allow(zk_client).to receive(:read_data).with('/ktl/reassign').and_return([reassignment_json])
          allow(zk_client).to receive(:read_data).with('/admin/reassign_partitions').and_return([reassignment_json])
        end

        before do
          progress.display(shell)
        end

        it 'prints a message about remaining reassignments' do
          expect(logger).to have_received(:info).with('remaining partitions to reassign: 1 (0.00% done)')
        end

        context 'with :verbose => true' do
          let :options do
            {verbose: true}
          end

          it 'prints a table with the remaining reassignments' do
            expect(shell).to have_received(:print_table).with([
              %w[topic assignments],
              ['topic1', '0 => [0]']
            ], anything)
          end
        end
      end

      context 'when the current reassignment process is done' do
        let :reassignment_json do
          {
            'partitions' => [
              {'topic' => 'topic1', 'partition' => 0, 'replicas' => [0]},
            ]
          }.to_json
        end

        before do
          allow(zk_client).to receive(:read_data).with('/ktl/reassign').and_return([reassignment_json])
          allow(zk_client).to receive(:read_data).with('/admin/reassign_partitions').and_raise(ZkClient::Exception::ZkNoNodeException.new)
        end

        it 'prints a message about it being done' do
          progress.display(shell)
          expect(logger).to have_received(:info).with('no partitions remaining to reassign')
        end
      end

      context 'when there\'s a diff' do
        let :reassignment_json do
          {
            'partitions' => [
              {'topic' => 'topic2', 'partition' => 0, 'replicas' => [0]},
            ]
          }.to_json
        end

        let :original_json do
          {
            'partitions' => [
              {'topic' => 'topic2', 'partition' => 0, 'replicas' => [0]},
              {'topic' => 'topic1', 'partition' => 0, 'replicas' => [0]},
            ]
          }.to_json
        end

        before do
          allow(zk_client).to receive(:read_data).with('/ktl/reassign').and_return([original_json])
          allow(zk_client).to receive(:read_data).with('/admin/reassign_partitions').and_return([reassignment_json])
        end

        before do
          progress.display(shell)
        end

        it 'prints a message about remaining reassignments' do
          expect(logger).to have_received(:info).with('remaining partitions to reassign: 1 (50.00% done)')
        end

        context 'with :verbose => true' do
          let :options do
            {verbose: true}
          end

          it 'prints a table with the remaining reassignments' do
            expect(shell).to have_received(:print_table).with([
              %w[topic assignments],
              ['topic2', '0 => [0]']
            ], anything)
          end
        end
      end
    end
  end
end
