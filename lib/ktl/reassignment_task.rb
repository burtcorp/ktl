# encoding: utf-8

module Ktl
  class ReassignmentTask
    def initialize(reassigner, plan, shell)
      @reassigner = reassigner
      @plan = plan
      @shell = shell
    end

    def execute
      if @reassigner.reassignment_in_progress?
        @shell.say 'Reassignment already in progress, exiting', :red
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

