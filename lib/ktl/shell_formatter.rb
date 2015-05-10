# encoding: utf-8

module Ktl
  class ShellFormater < Logger::Formatter
    def initialize(shell)
      @shell = shell
      @padding = ' ' * 2
    end

    def call(severity, time, progname, msg)
      severity_part = sprintf('%-5s', severity.downcase)
      severity_part = @shell.set_color(severity_part, color_for[severity])
      line = %(#{@padding}#{severity_part} #{msg2str(msg)})
      line << NEWLINE unless line.end_with?(NEWLINE)
      line
    end

    private

    NEWLINE = "\n".freeze

    def color_for
      @color_for ||= {
        'DEBUG' => :cyan,
        'INFO' => :magenta,
        'WARN' => :yellow,
        'ERROR' => :red,
        'FATAL' => :red,
      }
    end
  end
end
