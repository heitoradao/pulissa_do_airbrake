#!/usr/bin/env ruby
# This script loads error information from airbrake and do git blame on them.
# HAJ


# $: << 'lib'

require 'pry'
require 'faraday'
require 'json'

Dir.glob('lib/*.rb').each { |lib| load lib }

require 'dotenv'
Dotenv.load

def get_cached_json
  File.read('data/20220404-1703-alia-groups.json')
end

def get_live_info
  key = ENV['AIRBRAKE_KEY']
  project_id = ENV['PROJECT_ID']
  url = "https://api.airbrake.io/api/v4/projects/#{project_id}/groups?key=#{key}"
  response = Faraday.get(url)
  response.body
end

h = JSON.parse(get_cached_json)

begin
  h = JSON.parse(raw_json)
  groups = h['groups'].map {|g| Group.new(g) }
  my_fault = groups.select do |g|
    #g.blame_include?('Heitor') ||
    g.files_include?('advance')
  end

  binding.pry
rescue JSON::ParserError => e
  puts e.message
end
