# encoding: utf-8

module Ktl
  module Control
    class Reassignment
      def initialize(reassigner, plan, shell)
        @reassigner = reassigner
        @plan = plan
        @shell = shell
      end

      def perform
        if @reassigner.in_progress?
          in_progress = @reassigner.partitions.size
          @shell.say 'Reassignment already in progress, %d partitions remaining' % in_progress
        else
          if use_overflow?
            @shell.say 'Loading overflow data'
            reassignment = @reassigner.load_overflow
          else
            @shell.say 'Generating a new reassignment plan'
            reassignment = @plan.generate
          end
          if reassignment.size > 0
            @shell.say 'Reassigning %d partitions' % reassignment.size
            @reassigner.execute(reassignment)
          else
            @shell.say 'Empty reassignment, ignoring'
          end
        end
      end

      private

      def use_overflow?
        @reassigner.overflow? && @shell.yes?('Overflow from previous reassignment found, use? [y/n]: ')
      end
    end
  end
end
