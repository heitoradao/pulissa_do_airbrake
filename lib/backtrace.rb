# frozen_string_literal: true

class Backtrace
  attr_accessor :file, :function, :line, :column, :code

  def initialize(data)
    self.file = data['file']
    self.function = data['function']
    self.line = data['line']
    self.column = data['column']
    self.code = data['code']
  end

  def remove_prefix(file)
    #start = @file.index('app')
    file[14..-1]
  end

  def get_blame
  end
end

