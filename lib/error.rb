# frozen_string_literal: true

class Error
  attr_accessor :type, :message, :backtrace

  def initialize(data)
    self.type = data['type']
    self.message = data['message']
    self.backtrace = Array.new(data['backtrace'].size) {|i| Backtrace.new(data['backtrace'][i]) }
  end
end
