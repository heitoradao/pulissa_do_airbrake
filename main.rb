require 'pry'
require 'faraday'
require 'json'
require 'date'
require 'faker'
require 'active_support'
require 'dotenv'
Dotenv.load

Dir.glob('lib/**/*.rb').each { |lib| load lib }

Ui::Main.introduction
option_number = Ui::Main.menu

Ui::Main.presentation_of_option(option_number)
