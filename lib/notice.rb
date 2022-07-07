require 'active_support/all'

class Notice
  attr_accessor :data

  def initialize(data)
    self.data = data

    data.each do |k, v|
      snakefyed = k.underscore
      class_eval("attr_accessor :#{snakefyed}")
      send(:"#{snakefyed}=", v)
    end

    self.errors.map! {|e| Error.new(e) }
  end

  def self.fetch_notices(project_id, group_id)
    cache_filename = 'data/notices.json.cache'
    raw = ''

    if File.exist?(cache_filename)
      raw = File.read cache_filename
    else
      url = "https://api.airbrake.io/api/v4/projects/#{project_id}/groups/#{group_id}/notices"
      params = { key: ENV['AIRBRAKE_KEY'] }
      response = Faraday.get(url, params)
      raw = response.body
      File.write(cache_filename, response.body)
    end

    JSON.parse(raw)['notices'].map {|n| Notice.new(n) }
  end
end
