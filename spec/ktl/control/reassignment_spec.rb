# encoding: utf-8

require 'spec_helper'


module Ktl
  module Control
    describe Reassignment do
      let :control do
        described_class.new(reassigner, plan, shell)
      end

      let :reassigner do
        double(:reassigner, execute: nil, in_progress?: false, overflow?: false, load_overflow: {})
      end

      let :plan do
        double(:plan, generate: {})
      end

      let :shell do
        double(:shell, yes?: nil, say: nil)
      end

      describe '#perform' do
        context 'when a reassignment is already in progress' do
          before do
            allow(reassigner).to receive(:partitions).and_return(double(size: 2))
            allow(reassigner).to receive(:in_progress?).and_return(true)
            allow(shell).to receive(:say)
          end

          it 'prints a message to the user' do
            control.perform
            expect(shell).to have_received(:say).with('Reassignment already in progress, 2 partitions remaining')
          end
        end

        context 'when there\'s an overflow file present' do
          it 'asks the user whether to use it or not' do
            allow(reassigner).to receive(:overflow?).and_return(true)
            control.perform
            expect(shell).to have_received(:yes?).with('Overflow from previous reassignment found, use? [y/n]: ')
          end

          context 'when the user answers `y`/`yes`' do
            before do
              allow(reassigner).to receive(:overflow?).and_return(true)
              allow(shell).to receive(:yes?).and_return(true)
              allow(reassigner).to receive(:load_overflow).and_return({overflow: true})
            end

            it 'uses the content of the overflow file' do
              control.perform
              expect(reassigner).to have_received(:execute).with({overflow: true})
            end

            it 'prints a message about loading overflow data' do
              control.perform
              expect(shell).to have_received(:say).with('Loading overflow data')
            end
          end

          context 'when the user answers something else' do
            before do
              allow(reassigner).to receive(:overflow?).and_return(true)
              allow(shell).to receive(:yes?).and_return(false)
              allow(plan).to receive(:generate).and_return({generated: 'plan'})
            end

            it 'executes a newly generated plan' do
              control.perform
              expect(reassigner).to have_received(:execute).with({generated: 'plan'})
            end

            it 'prints a message about generating a new plan' do
              control.perform
              expect(shell).to have_received(:say).with('Generating a new reassignment plan')
            end
          end
        end

        context 'when there\'s no overflow file present' do
          it 'executes a newly generated plan' do
            allow(plan).to receive(:generate).and_return({generated: 'new-plan'})
            control.perform
            expect(reassigner).to have_received(:execute).with({generated: 'new-plan'})
          end

          it 'prints a message about generating a new plan' do
            control.perform
            expect(shell).to have_received(:say).with('Generating a new reassignment plan')
          end
        end

        context 'when the reassignment is empty' do
          it 'prints a message to the user' do
            control.perform
            expect(shell).to have_received(:say).with('Empty reassignment, ignoring')
          end

          it 'does not execute any reassignment' do
            control.perform
            expect(reassigner).to_not have_received(:execute)
          end
        end
      end
    end
  end
end
