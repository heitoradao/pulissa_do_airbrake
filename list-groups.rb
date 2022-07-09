#!/usr/bin/env ruby
# This script loads error information from airbrake and do git blame on them.
# HAJ


# $: << 'lib'

def get_cached_json
  File.read('data/cached.json')
end

def get_live_info
  key = ENV['AIRBRAKE_KEY']
  project_id = nil
  params = { key: key }
  # url = "https://api.airbrake.io/api/v4/groups"
  url = "https://api.airbrake.io/api/v4/projects/#{project_id}/groups"
  response = Faraday.get(url, params)
  File.write('./data/cached.json', response.body)
  response.body
end

raw_json = get_live_info
#raw_json = get_cached_json

begin
  h = JSON.parse(raw_json)
  groups = h['groups'].map {|g| Group.new(g) }
  my_fault =
    groups.select do |g|
      #g.blame_include?('Heitor') ||
      g.files_include?('advance')
    end

rescue JSON::ParserError => e
  puts e.message
end
