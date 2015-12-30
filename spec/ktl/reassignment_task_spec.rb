# encoding: utf-8

require 'spec_helper'


module Ktl
  describe ReassignmentTask do
    let :task do
      described_class.new(reassigner, plan, shell, logger: logger)
    end

    let :reassigner do
      double(:reassigner, execute: nil, reassignment_in_progress?: false, overflow?: false, limit: nil, load_overflow: {})
    end

    let :plan do
      double(:plan, generate: {})
    end

    let :shell do
      double(:shell, yes?: nil, say: nil)
    end

    let :logger do
      double(:logger, debug: nil, info: nil, warn: nil, error: nil)
    end

    describe '#execute' do
      context 'when a reassignment is already in progress' do
        before do
          allow(reassigner).to receive(:partitions).and_return(double(size: 2))
          allow(reassigner).to receive(:reassignment_in_progress?).and_return(true)
        end

        it 'prints a message to the user' do
          task.execute
          expect(logger).to have_received(:warn).with('reassignment already in progress, exiting')
        end
      end

      context 'when there\'s overflow present' do
        it 'asks the user whether to use it or not' do
          allow(reassigner).to receive(:overflow?).and_return(true)
          task.execute
          expect(logger).to have_received(:info).with(/overflow from previous reassignment found, use?/)
          expect(shell).to have_received(:yes?).with(/\s+>/)
        end

        context 'when the user answers `y`/`yes`' do
          before do
            allow(reassigner).to receive(:overflow?).and_return(true)
            allow(shell).to receive(:yes?).and_return(true)
            allow(reassigner).to receive(:load_overflow).and_return({overflow: true})
          end

          it 'uses the content of the overflow' do
            task.execute
            expect(reassigner).to have_received(:execute).with({overflow: true})
          end

          it 'prints a message about loading overflow data' do
            task.execute
            expect(logger).to have_received(:info).with('loading overflow data')
          end
        end

        context 'when the user answers something else' do
          before do
            allow(reassigner).to receive(:overflow?).and_return(true)
            allow(shell).to receive(:yes?).and_return(false)
            allow(plan).to receive(:generate).and_return({generated: 'plan'})
          end

          it 'executes a newly generated plan' do
            task.execute
            expect(reassigner).to have_received(:execute).with({generated: 'plan'})
          end

          it 'prints a message about generating a new plan' do
            task.execute
            expect(logger).to have_received(:info).with('generating a new reassignment plan')
          end
        end
      end

      context 'when there\'s no overflow present' do
        it 'executes a newly generated plan' do
          allow(plan).to receive(:generate).and_return({generated: 'new-plan'})
          task.execute
          expect(reassigner).to have_received(:execute).with({generated: 'new-plan'})
        end

        it 'prints a message about generating a new plan' do
          task.execute
          expect(logger).to have_received(:info).with('generating a new reassignment plan')
        end
      end

      context 'when the reassignment is empty' do
        it 'prints a message to the user' do
          task.execute
          expect(logger).to have_received(:warn).with('empty reassignment, ignoring')
        end

        it 'does not execute any reassignment' do
          task.execute
          expect(reassigner).to_not have_received(:execute)
        end
      end
    end
  end
end
