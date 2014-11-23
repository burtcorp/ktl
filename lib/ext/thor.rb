# encoding: utf-8

class Thor
  module Shell
    class Basic

      protected

      def unix?
        (RUBY_PLATFORM =~ /java/i && RbConfig::CONFIG['host_os'] =~ /darwin|linux/i) || super
      end
    end
  end
end
