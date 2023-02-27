require 'securerandom'
require 'sidekiq/shard_set'
require 'sidekiq/batch/callback'
require 'sidekiq/batch/client'
require 'sidekiq/batch/middleware'
require 'sidekiq/batch/status'

module Sidekiq
  ##
  # Provide a higher-level Batch abstraction for units of work.
  # Given a set of work, we want to break the set down to individual jobs
  # for Sidekiq to process in parallel but then have an overall
  # notification when the entire set is complete.
  #
  #   batch = Sidekiq::Batch.new
  #   batch.on(:complete, self.class, :to => current_user.email)
  #   batch.jobs do
  #     # push messages to sidekiq
  #   end
  #
  # Sidekiq generates a unique Batch ID, along with the number of jobs pushed
  # in the batch.
  #
  # Batches may be nested by creating a new Batch within another batch's +jobs+
  # method.  When the child batch runs an event callback, it checks to see if
  # it needs to fire the parent batch's event callback too.

  class Batch

    def self.redis(bid, &block)
      idx = 0
      m = bid.match(/@@(\d+)/)
      idx = m[1].to_i if m
      Sidekiq::Shards.on(idx) do
        Sidekiq.redis(&block)
      end
    end

    def redis(bid, &block)
      self.class.redis(bid, &block)
    end

    class NoSuchBatch < StandardError; end

    ONE_DAY = 60 * 60 * 24
    EXPIRY = ONE_DAY * 30

    # Controls how long a batch record "lingers" in Redis before expiring.
    # This allows APIs like Status#poll to check batch status even after
    # the batch succeeds and is no longer needed.  You can lower this
    # constant if you create lots of batches, want to reclaim the memory
    # and don't use polling.
    LINGER = ONE_DAY
    VALID_EVENTS = %w(complete success)

    attr_reader :created_at
    attr_reader :bid
    attr_reader :callbacks
    attr_reader :parent_bid
    attr_accessor :description
    attr_accessor :callback_queue

    def initialize(bid=nil)
      @expiry = EXPIRY
      if bid
        @bid = bid
        @key = "b-#{bid}"

        props = redis(bid) do |conn|
          conn.hgetall(@key)
        end
        raise NoSuchBatch, "Couldn't find Batch #{@bid} in redis" unless props['callbacks']
        raise "Batch #{@bid} has finished, you cannot modify it anymore" if props['deleted']
        @created_at = props['created_at'.freeze].to_f
        @description = props['description'.freeze]
        @parent_bid = props['parent']
        @callbacks = Sidekiq.load_json(props['callbacks'])
        @mutable = false
        @new = false
      else
        @bid = SecureRandom.urlsafe_base64(10)

        ss = Sidekiq::Shards
        if ss.enabled?
          @shard = ss.random_index
          @bid += "@@#{@shard}"
        else
          @shard = 0
        end

        @key = "b-#{@bid}"
        @created_at = Time.now.utc.to_f
        @callbacks = {}
        @mutable = true
        @new = true
      end
    end

    def parent
      Batch.new(parent_bid) if parent_bid
    end

    def expiry
      @expiry || EXPIRY
    end

    def expires_at
      Time.at(@created_at + expiry)
    end

    # Retrieve the current set of JIDs associated with this batch.
    def jids
      redis(bid) do |conn|
        conn.smembers("b-#{bid}-jids")
      end
    end

    def include?(jid)
      redis(bid) do |conn|
        conn.sismember("b-#{bid}-jids", jid)
      end
    end
    alias_method :valid?, :include?

    def invalidate_all
      result, _ = redis(bid) do |conn|
        conn.multi do
          conn.del("b-#{bid}-jids")
          conn.hset(@key, "invalid", -1)
        end
      end
      result
    end

    def invalidate_jids(*jids)
      count, _ = redis(bid) do |conn|
        conn.multi do
          conn.srem("b-#{bid}-jids", jids)
          conn.hincrby(@key, "invalid", jids.size)
        end
      end
      count
    end

    def invalidated?
      count = redis(bid) do |conn|
        conn.hget(@key, "invalid").to_i
      end
      count != 0
    end

    def status
      Status.new(@bid)
    end

    def mutable?
      !!@mutable
    end

    ##
    # Call a method upon completion or success of a batch.  You
    # may pass a bare Class, which will call "on_#{event}", or a
    # String with the exact 'Class#method' to call.
    #
    #   batch.on(:complete, MyClass)
    #   batch.on(:success, 'MyClass#foo')
    #   batch.on(:complete, MyClass, :email => current_user.email)
    #
    # The Class should implement a method signature like this:
    #
    #   def on_complete(status, options)
    #   end
    #
    def on(event, call, options={})
      raise "Batch cannot be modified, jobs have already been defined" unless @mutable
      e = event.to_s
      raise ArgumentError, "Invalid event name: #{e}" unless VALID_EVENTS.include?(e)

      @callbacks ||= {}
      @callbacks[e] ||= []
      @callbacks[e] << { call => options }
    end

    ##
    # Pass in a block which pushes all the work associated
    # with this batch to Sidekiq.
    #
    # Returns the set of JIDs added to the batch.
    #
    # Note: all jobs defined within the block are pushed to Redis atomically
    # so either the entire set of jobs are defined successfully or none at all.
    def jobs(&block)
      raise ArgumentError, "Must specify a block" if !block
      parent_payloads, Thread.current[:sidekiq_batch_payloads] = Thread.current[:sidekiq_batch_payloads], []
      begin
        myparent = nil
        if mutable?
          # Brand new batch, persist data to Redis.
          data = ['created_at'.freeze, created_at,
                  'callbacks'.freeze, Sidekiq.dump_json(callbacks),
                  'description'.freeze, description]
          if self.callback_queue
            data << 'cbq'.freeze
            data << self.callback_queue
          end
          if Thread.current[:sidekiq_batch]
            @parent_bid = myparent = Thread.current[:sidekiq_batch].bid
            data << 'parent'.freeze
            data << myparent
          end
          Sidekiq::Pro.metrics.increment("batches.create")

          redis(bid) do |conn|
            conn.multi do
              conn.hmset(@key, *data)
              # Default expiry is one day for newly created batches.
              # If jobs are added to the batch, it is extended to 30 days.
              conn.expire(@key, ONE_DAY)
            end
          end
        end

        @mutable = false
        @added = []

        begin
          parent, Thread.current[:sidekiq_batch] = Thread.current[:sidekiq_batch], self
          block.call
        ensure
          Thread.current[:sidekiq_batch] = parent
        end

        # If the jobs block produces no jobs, exit early.
        return (Sidekiq.logger.debug("Skipping empty batch #{@key}"); []) unless @added.size > 0

        # Here's what we've been waiting for.  We delay all
        # batch chatter with Redis so we can send everything
        # in one big atomic MULTI push.
        redis(bid) do |conn|
          conn.multi do
            # As an optimization, don't continually zadd and zremrangebyscore when we're adding jobs
            # to an existing batch
            if @new
              conn.zremrangebyscore('batches'.freeze, '-inf'.freeze, Time.now.to_f)
              conn.zadd('batches'.freeze, @created_at + expiry, bid)
            end
            # only want to incr kids when the batch is first created,
            # not when reopened to add more jobs.
            if myparent
              conn.hincrby("b-#{myparent}", "kids".freeze, 1)
              conn.expire("b-#{myparent}", EXPIRY)
            end
            conn.expire @key, EXPIRY

            if !immediate_registration?
              increment_batch_jobs_to_redis(conn, @added)
            end

            if !Sidekiq::Shards.enabled?
              Sidekiq::Client.new.flush(conn)
            end
          end
        end
        @new = false

        if Sidekiq::Shards.enabled?
          client = Sidekiq::Client.new
          client.redis_pool.with do |conn|
            client.flush(conn)
          end
        end
        @added
      ensure
        Thread.current[:sidekiq_batch_payloads] = parent_payloads
      end
    end

    # Not a public API
    def register(jid) # :nodoc:
      if immediate_registration?
        redis(bid) do |conn|
          conn.multi do
            increment_batch_jobs_to_redis(conn, [jid])
          end
        end
      end
      @added << jid
    end

    private

    def immediate_registration?
      defined?(Sidekiq::Testing) && Sidekiq::Testing.inline?
    end

    def increment_batch_jobs_to_redis(conn, jids)
      jids_key = "b-#{bid}-jids"

      Sidekiq.logger.debug { "Adding #{jids.size} jobs to batch #{bid}, JIDs #{jids}" }
      conn.hincrby(@key, "pending".freeze, jids.size)
      conn.hincrby(@key, "total".freeze, jids.size)
      conn.sadd(jids_key, jids)
      conn.expire(jids_key, expiry)
    end
  end
end
