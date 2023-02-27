require 'socket'
require 'sidekiq'

module Sidekiq

  ##
  # Limiter provides a rate limiting API for Ruby.  It does
  # not need to run within the Sidekiq server process.  Any
  # process configured to use the same Redis can use the
  # Sidekiq::Limiter API to rate limit their execution
  # with all other similarily-configured processes.
  #
  # Please note that this rate limiting API is +blocking+
  # and might cause your threads to come to a complete halt
  # if you are not careful how you allocate jobs between
  # queues and processes.
  #
  # This will register a rate limiter which only allows 50
  # blocks to execute concurrently.  If you have hundreds of
  # worker threads, this ensures they won't slam a remote
  # API if thousands of jobs are enqueued.
  #
  #   NETSUITE_THROTTLE = Sidekiq::Limiter.concurrent(:netsuite, 50,
  #                                                 lock_timeout: 30, wait_timeout: 5)
  #
  # This will register a rate limiter which only allows 5
  # operations per second.
  #
  #   limiter = Sidekiq::Limiter.quota(:stripe, 5, :second)
  #
  # You'd then use a limiter in any Ruby code like so:
  #
  #   limiter.within_limit do
  #     # this block will execute if the rate limit is met
  #   end
  #
  # All limiters are thread-safe and may be shared.
  #
  module Limiter
    LEGAL_NAME = /\A[A-Za-z_\-0-9]+\Z/

    DEFAULT_TTL = 90*24*60*60
    DEFAULT_OPTIONS = {
      lock_timeout: 30,
      wait_timeout: 5,
      policy: :raise,
      ttl: DEFAULT_TTL,
    }

    class OverLimit < ::RuntimeError
      attr_accessor :limiter
      def initialize(limiter)
        @limiter = limiter
      end

      def to_s
        limiter.to_s
      end
    end

    # Configure the Limiter subsystem:
    #
    # Sidekiq::Limiter.configure do |config|
    #   config.redis = { url: 'redis://localhost/0' }
    #   config.errors << Foo::Bar
    #   config.backoff = ->(a, b) do
    #     10
    #   end
    # end
    def self.configure
      yield self
    end

    def self.redis=(pool)
      @redis = if pool.is_a?(ConnectionPool)
        pool
      else
        Sidekiq::RedisConnection.create(pool)
      end
    end

    def self.redis(&block)
      raise ArgumentError, "requires a block" unless block_given?
      redis_pool.with do |conn|
        retryable = true
        begin
          yield conn
        rescue Redis::CommandError => ex
          #2550 Failover can cause the server to become a slave, need
          # to disconnect and reopen the socket to get back to the master.
          (conn.disconnect!; retryable = false; retry) if retryable && ex.message =~ /READONLY/
          raise
        end
      end
    end

    def self.redis_pool
      @redis ||= Sidekiq.redis_pool
    end

    ##
    # Register a concurrent rate limiter within Redis.
    #
    # Limit to 50 concurrent ERP operations:
    #
    #     Sidekiq::Limiter.concurrent(:erp, 50, wait_timeout: 10)
    #
    # Options:
    #   :lock_timeout - seconds before a concurrent lock is automatically released, necessary if a
    #     process crashes while holding a lock, default 30.  **Your concurrent operations
    #     must take less than this amount of time!**
    #   :wait_timeout - seconds to wait for the rate limit or raises a
    #     Sidekiq::Limiter::OverLimit, 0 means never wait, default 5
    #   :policy - what should the API do if rate limit cannot be met, legal values: [:raise, :skip],
    #     :raise will raise Sidekiq::Limiter::OverLimit, :skip will return without executing the block,
    #     defaults to :raise
    #
    def self.concurrent(name, count, options={})
      name = name.to_s
      raise ArgumentError, "Blank name?" if name == ''.freeze
      count = count.to_i
      options = DEFAULT_OPTIONS.merge(options)

      Sidekiq::Limiter::Concurrent.new(name, count, options)
    end

    ##
    # Register a bucket-based rate limiter within Redis.  Buckets can be
    # per :second, :minute, :hour or :day.
    #
    # Limit Stripe operations to 10 per second:
    #
    #     Sidekiq::Limiter.bucket(:stripe, 10, :second)
    #
    # Bucket means that you can perform 10 operations at 12:44:03.999 and
    # then another 10 operations at 12:44:04.000, because each interval is
    # considered a bucket.
    #
    # Options:
    #   :wait_timeout - seconds to wait for the rate limit or raises a
    #     Sidekiq::Limiter::OverLimit, 0 means never wait, default 5
    #   :policy - what should the API do if rate limit cannot be met, legal values: [:raise, :skip],
    #     :raise will raise Sidekiq::Limiter::OverLimit, :skip will return without executing the block,
    #     defaults to :raise
    #
    def self.bucket(name, count, interval, options={})
      name = name.to_s
      raise ArgumentError, "Blank name?" if name == ''.freeze
      options = DEFAULT_OPTIONS.merge(options)

      Sidekiq::Limiter::Bucket.new(name, count, interval, options)
    end

    ##
    # Register a sliding-window-based rate limiter within Redis.  The window can be
    # per :second, :minute, :hour or :day.
    #
    # Limit banking operations to 10 per second:
    #
    #     Sidekiq::Limiter.window(:banking, 10, :second)
    #
    # Options:
    #   :wait_timeout - seconds to wait for the rate limit or raises a
    #     Sidekiq::Limiter::OverLimit, 0 means never wait, default 5
    #   :policy - what should the API do if rate limit cannot be met, legal values: [:raise, :skip],
    #     :raise will raise Sidekiq::Limiter::OverLimit, :skip will return without executing the block,
    #     defaults to :raise
    #
    def self.window(name, count, interval, options={})
      name = name.to_s
      raise ArgumentError, "Blank name?" if name == ''.freeze
      options = DEFAULT_OPTIONS.merge(options)

      Sidekiq::Limiter::Window.new(name, count, interval, options)
    end

  end
end

require 'sidekiq-ent/limiter/bucket'
require 'sidekiq-ent/limiter/concurrent'
require 'sidekiq-ent/limiter/window'
require 'sidekiq-ent/limiter/middleware'
require 'sidekiq-ent/limiter/status'
