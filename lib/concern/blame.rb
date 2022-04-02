module Blamer
  attr_accessor :committer, :committed_at

  def fetch_blame

  end

  private

  def get_git_info
    command = ""
    data = `#{command}`
  end
end
