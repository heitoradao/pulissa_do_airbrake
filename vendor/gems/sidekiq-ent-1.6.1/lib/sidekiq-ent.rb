require 'sidekiq-pro'

Sidekiq.options[:lifecycle_events][:leader] = []

require 'sidekiq-ent/version'
require 'sidekiq-ent/scripting'
require 'sidekiq-ent/election'
require 'sidekiq-ent/limiter'
require 'sidekiq-ent/periodic'
require 'sidekiq-ent/unique'
require 'sidekiq-ent/metrics'
require 'sidekiq-ent/encryption'
require 'sidekiq-ent/census'

Sidekiq.send(:remove_const, :NAME)
Sidekiq::NAME = "Sidekiq Enterprise"
Sidekiq.send(:remove_const, :LICENSE)
Sidekiq::LICENSE = "Sidekiq Pro #{Sidekiq::Pro::VERSION} / Sidekiq Enterprise #{Sidekiq::Enterprise::VERSION}, commercially licensed."
