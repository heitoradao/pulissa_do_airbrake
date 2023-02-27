require 'sidekiq'
require 'sidekiq/pro/version'
require 'sidekiq/pro/worker'
require 'sidekiq/pro/api'
require 'sidekiq/pro/push'
require 'sidekiq/pro/util'
require 'sidekiq/pro/metrics'
require 'sidekiq/batch'

Sidekiq.send(:remove_const, :LICENSE)
Sidekiq.send(:remove_const, :NAME)
Sidekiq::NAME = "Sidekiq Pro"
Sidekiq::LICENSE = "Sidekiq Pro #{Sidekiq::Pro::VERSION}, commercially licensed.  Thanks for your support!"

Sidekiq.configure_server do
  class Sidekiq::CLI
    def self.banner
      File.read(File.expand_path(File.join(__FILE__, '../sidekiq/intro.ans')))
    end
  end
  require 'sidekiq/pro/basic_fetch'
  Sidekiq.options[:fetch] = Sidekiq::Pro::BasicFetch
end


# Enable various reliability add-ons:
#
#   Sidekiq.configure_server do |config|
#     config.reliable_fetch!
#     config.reliable_scheduler!
#     config.timed_fetch!
#     # enable both
#     config.reliable!
#   end
#
module Sidekiq
  def self.reliable_fetch!
    require 'sidekiq/pro/fetch'
    Sidekiq.options[:fetch] = Sidekiq::Pro::ReliableFetch
    Sidekiq.options[:ephemeral_hostname] ||= !!ENV['DYNO']
    env = Sidekiq.options[:environment]
    Sidekiq.options[:index] ||= 0 if !env || env == 'development'
    Array(Sidekiq.options[:labels]) << 'reliable'
    nil
  end

  def self.super_fetch!
    require 'sidekiq/pro/super_fetch'
    Sidekiq.options[:fetch] = Sidekiq::Pro::SuperFetch
    Array(Sidekiq.options[:labels]) << 'reliable'
    nil
  end

  def self.timed_fetch!(timeout = 3600)
    require 'sidekiq/pro/timed_fetch'
    Sidekiq.options[:fetch] = Sidekiq::Pro::TimedFetch
    Array(Sidekiq.options[:labels]) << 'reliable'

    Sidekiq.configure_server do |config|
      config.on(:startup) do
        klass = Sidekiq::Pro::TimedFetch::Manager
        klass.instance = klass.new(Sidekiq.options)
        klass.instance.timeout = timeout
      end
    end
    nil
  end

  def self.reliable_scheduler!
    require 'sidekiq/pro/scheduler'
    Sidekiq.options[:scheduled_enq] = Sidekiq::Scheduled::FastEnq
  end

  def self.reliable!
    reliable_fetch!
    reliable_scheduler!
  end

  def self.redis_pool
    # Slight tweak to allow sharding support
    Thread.current[:sidekiq_redis_pool] || (@redis ||= Sidekiq::RedisConnection.create)
  end
end
