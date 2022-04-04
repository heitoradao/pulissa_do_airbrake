# frozen_string_literal: true

class Group
  attr_accessor :id, :project_id
  attr_accessor :resolved, :muted, :muted_by, :muted_at
  attr_accessor :errors
  attr_accessor :context
  attr_accessor :last_notice_id
  attr_accessor :last_notice_at, :notice_count, :notice_total_count
  attr_accessor :comment_count

  def initialize(data)
    self.id = data['id']
    self.project_id = data['projectId']
    self.resolved = data['resolved']
    self.errors = Array.new(data['errors'].size) {|i| Error.new(data['errors'][i]) }
    self.context = data['context']
    self.last_notice_id = data['lastNoticeId']
    self.last_notice_at = data['lastNoticeAt']
    self.notice_count = data['noticeCount']
    self.notice_total_count = data['noticeTotalCount']
  end
end

