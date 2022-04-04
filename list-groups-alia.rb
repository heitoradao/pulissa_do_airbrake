#!/usr/bin/env ruby
# This script loads error information from airbrake and do git blame on them.
# HAJ


# $: << 'lib'

require 'pry'
require 'faraday'
require 'json'


Dir.glob('lib/*.rb').each { |lib| load lib }

require 'dotenv'
Dotenv.load('airbrake.env')

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


def filter_file(filename)
  start = filename.index('app')
  filename[start..-1]
end

def get_blame(filename, line)
  platform_path = ENV['PLATFORM_PATH']
  fixed_filename = filter_file(filename)
  command = "git -C #{platform_path} blame #{fixed_filename} -L #{line},#{line}"
  `#{command}`
end

binding.pry

