require 'pry'
require 'faraday'
require 'json'
require 'date'
require 'active_support'
require 'dotenv'
Dotenv.load

Dir.glob('lib/**/*.rb').each { |lib| load lib }

Ui::Main.introduction