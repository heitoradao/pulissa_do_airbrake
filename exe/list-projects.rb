#!/usr/bin/env ruby

require 'faraday'
require 'dotenv'
require 'pry'

Dotenv.load

# monitored_projects_names = ['Platform Jobs', 'Platform API', 'Platform Hook-Receivers', 'notifier', 'Alia']

url = "https://api.airbrake.io/api/v4/projects"
response = Faraday.get(url, { key: ENV['AIRBRAKE_KEY'] })
h = JSON.parse(response.body)

projects = h['projects'].map{|p| OpenStruct.new(p) }

binding.pry

