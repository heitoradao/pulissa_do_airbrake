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

  def is_platform?
    # TODO: move to other class, since it's specific to my project
    file.start_with?('/PROJECT_ROOT/')
  end

  def remove_prefix(file)
    #start = @file.index('app')
    # 14 == '/PROJECT_ROOT/'.size
    file[14..-1]
  end

  def get_blame
    if is_platform?
      codebase_path = ENV['CODEBASE_PATH']
      command = "git -C #{codebase_path} blame #{remove_prefix(file)} -L #{line},#{line}"
      `#{command}`
    end
  end
end
