# frozen_string_literal: true

class Project
  attr_accessor :id, :name, :file_count, :groups

  def initialize(data)
    self.id = data['id']
    self.name = data['name']
    self.file_count = data['fileCount']
    self.groups = Array.new(data['groups'].size) {|i| Group.new(data['groups'][i]) }
  end
end

