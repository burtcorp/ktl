# encoding: utf-8

module Ktl
  class ReassignmentTask
    def initialize(reassigner, plan, shell, options={})
      @reassigner = reassigner
      @plan = plan
      @shell = shell
      @logger = options[:logger] || NullLogger.new
    end

    def execute(dryrun = false)
      if @reassigner.reassignment_in_progress?
        @logger.warn 'reassignment already in progress, exiting'
      else
        if use_overflow?
          @logger.info 'loading overflow data'
          reassignment = @reassigner.load_overflow
        else
          @logger.info 'generating a new reassignment plan'
          reassignment = @plan.generate
        end
        if reassignment.size > 0
          @logger.info 'reassigning %d partitions' % reassignment.size
          if dryrun
            @logger.info 'dryrun detected, skipping reassignment'
          else
            @reassigner.execute(reassignment)
          end
        else
          @logger.warn 'empty reassignment, ignoring'
        end
      end
    end

    private

    def use_overflow?
      if @reassigner.overflow?
        @logger.info 'overflow from previous reassignment found, use? [y/n]'
        @shell.yes? ' ' * 8 << '>'
      end
    end
  end
end

