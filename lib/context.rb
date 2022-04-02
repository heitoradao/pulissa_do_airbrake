# frozen_string_literal: true

class Context
  attr_accessor :action, :component, :environment, :severity

  def initialize(data)
    self.action = data['action']
    self.component = data['component']
    self.environment = data['environments']
    self.severity = data['severity']
  end
end
