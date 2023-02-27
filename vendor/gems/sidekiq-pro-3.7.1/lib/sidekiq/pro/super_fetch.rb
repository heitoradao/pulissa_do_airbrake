require 'securerandom'
require 'concurrent'
require 'set'
require 'sidekiq/pro/config'

module Sidekiq::Pro

  ##
  # Provides reliable queue processing via Redis' rpoplpush command.
  #
  # 1. retrieve the work while pushing it to our private queue for this process.
  # 2. process the work
  # 3. acknowledge the work by removing it from our private queue
  #
  class SuperFetch
    include Sidekiq::Util

    def initialize(retriever=Retriever.instance, options)
      @retriever = retriever

      begin
        self.class.check_for_orphans if self.class.orphan_check?(options)
      rescue SystemCallError
        # orphan check is best effort, we don't want Redis downtime to
        # break Processor
      rescue => ex
        Sidekiq.logger.warn(ex)
      end
    end

    def retrieve_work
      @retriever.retrieve_work
    end

    def self.bulk_requeue(in_progress, options)
      # Ignore the in_progress arg passed in; rpoplpush lets us know everything in process
      Sidekiq.redis do |conn|
        get_queues(options).each do |(queue, working_queue)|
          while conn.rpoplpush(working_queue, queue)
            Sidekiq.logger.info {"SuperFetch: Moving job from #{working_queue} back to #{queue}"}
          end
        end
        id = options[:identity]
        Sidekiq.logger.debug { "SuperFetch: Unregistering super process #{id}" }
        conn.multi do
          conn.srem("super_processes", id)
          conn.del("#{id}:super_queues")
        end
      end
    rescue => ex
      # best effort, ignore Redis network errors
      Sidekiq.logger.warn { "SuperFetch: Failed to requeue: #{ex.message}" }
    end

    def self.private_queue(q, options)
      "queue:sq|#{options[:identity]}|#{q}"
    end

    def self.get_queues(options)
      options[:queues].map {|q| ["queue:#{q}", private_queue(q, options)] }
    end

    def self.orphan_check?(options)
      delay = options.fetch(:super_fetch_orphan_check, 3600).to_i
      return false if delay == 0

      Sidekiq.redis do |conn|
        conn.set("super_fetch_orphan_check", Time.now.to_f, ex: delay, nx: true)
      end
    end

    # This method is extra paranoid verification to check Redis for any possible
    # orphaned queues with jobs.
    def self.check_for_orphans
      orphans = 0
      qcount = 0
      qs = Set.new
      Sidekiq.redis do |conn|
        ids = conn.smembers("super_processes")
        Sidekiq.logger.debug("SuperFetch found #{ids.size} super processes")

        conn.scan_each(:match => "queue:sq|*", :count => 100) do |que|
          qcount += 1
          _, id, name = que.split("|")
          next if ids.include?(id)

          # Race condition in pulling super_processes and checking queue liveness.
          # Need to verify in Redis.
          if !conn.sismember("super_processes", id)
            qs << name
            while conn.rpoplpush(que, "queue:#{name}")
              orphans += 1
            end
          end

        end
      end

      if orphans > 0
        Sidekiq::Pro.metrics.increment("jobs.recovered.fetch", orphans)
        Sidekiq.logger.warn { "SuperFetch recovered #{orphans} orphaned jobs in queues: #{qs.to_a.inspect}" }
      else
        Sidekiq.logger.info { "SuperFetch found #{qcount} working queues with no orphaned jobs" } if qcount > 0
      end
      orphans
    end

    # Each Processor thread calls #retrieve_work concurrently. Since our
    # reliable queue check is pretty heavyweight, we map all calls to #retrieve_work
    # onto a single thread using a C::TPE.  This singleton encapsulates the
    # single thread and call to Redis.
    class Retriever
      include Sidekiq::Util
      attr_accessor :options
      attr_accessor :paused
      class << self
        attr_accessor :instance
      end

      def initialize
        @paused = Set.new
        @internal = []
        @done = false
        @changed = true
      end

      def listen_for_pauses(events=Sidekiq::Pro::Config)
        members = Sidekiq.redis do |conn|
          conn.smembers("paused")
        end
        @paused = Set.new(Array(members))
        @changed = true
        events.register(self)
      end

      def notify(verb, payload)
        if verb == :pause
          @paused << payload
          @changed = true
        elsif verb == :unpause
          @paused.delete payload
          @changed = true
        end
      end

      def start(options)
        @options = options
        @pool ||= Concurrent::ThreadPoolExecutor.new(
          min_threads: 1,
          max_threads: 1,
          max_queue: options[:concurrency],
        )
        @queues = SuperFetch.get_queues(@options)
        @algo = (options[:strict] && @queues.length == @queues.uniq.length) ? Strict : Weighted

        Sidekiq.configure_server do |config|
          config.on(:startup) do
            @pool.post(&method(:startup))
          end
          config.on(:shutdown) do
            self.terminate
          end
        end
      end

      def terminate
        @done = true
        @pool.shutdown
      end

      def cleanup_the_dead
        count = 0
        Sidekiq.redis do |conn|
          conn.sscan_each("super_processes") do |x|
            next if conn.exists(x)

            Sidekiq.logger.debug { "SuperFetch: Cleaning up old super process #{x}" }

            # heartbeat has expired, push back any leftover jobs in private queues
            qs = conn.smembers("#{x}:super_queues")
            qs.each do |priv|
              (_, _, q) = priv.split("|")
              while conn.rpoplpush(priv, "queue:#{q}")
                count += 1
              end
            end

            conn.del("#{x}:super_queues")
            conn.srem("super_processes", x)
          end
        end
        Sidekiq.logger.warn("SuperFetch: recovered #{count} jobs") if count > 0
        count
      end

      def wait_for_heartbeat
        beats = 0
        while !Sidekiq.redis {|conn| conn.exists(identity) }
          # We want our own heartbeat to register before we
          # can register ourself
          sleep 0.1
          beats += 1
          raise "Did not find our own heartbeat within 10 seconds, that's bad" if beats > 100
        end unless $TESTING
        beats
      end

      def register_myself
        # We're officially alive in Redis so we can safely
        # register this process as a super process!
        qs = @queues.map{|x, priv| priv }
        # This method will run multiple times so seeing this message twice is no problem.
        Sidekiq.logger.debug { "SuperFetch: Registering super process #{identity} with #{qs}" }

        Sidekiq.redis do |conn|
          conn.multi do
            conn.sadd("super_processes", identity)
            conn.sadd("#{identity}:super_queues", qs)
          end
        end
      end

      def startup
        watchdog("SuperFetch#startup") do
          Sidekiq.logger.info("SuperFetch activated")

          Sidekiq.on(:heartbeat) do
            register_myself
          end

          cleanup_the_dead
          wait_for_heartbeat
          register_myself
        end
      end

      def retrieve_work
        return nil if @done
        begin
          future = Concurrent::Future.execute(:executor => @pool, &method(:get_job))
          val = future.value(nil)
          return val if val
          raise future.reason if future.rejected?
        rescue Concurrent::RejectedExecutionError
          # shutting down race condition, #2827, nbd
        end
      end

      def get_job
        return nil if @done
        @algo.call(active_queues)
      end

      private unless $TESTING

      def active_queues
        if @changed
          @queues = (@options[:queues] - @paused.to_a).map {|q| ["queue:#{q}", SuperFetch.private_queue(q, @options)] }
          @changed = nil
        end
        @queues
      end

      # In a weighted ordering, treat the queues like we're drawing
      # a queue out of a hat: draw a queue, attempt to fetch work.
      # Draw another queue, attempt to fetch work.
      Weighted = lambda {|queues|
        queues = queues.shuffle.uniq
        Strict.call(queues)
      }

      Strict = lambda {|queues|
        work = nil
        Sidekiq.redis do |conn|
          if queues.length > 1
            queues.each do |(queue, working_queue)|
              result = conn.rpoplpush(queue, working_queue)
              if result
                work = UnitOfWork.new(queue, result, working_queue)
                break
              end
            end
          end
          if work.nil?
            queue, working_queue = queues.first
            if queue
              # On the last queue, block to avoid spinning 100% of the CPU checking for jobs thousands of times per
              # second when no jobs are enqueued at all. The above shuffle will randomize the queue blocked on each time.
              # Queues with higher weights should still get blocked on more frequently since they should end up as the
              # last queue in queues more frequently.
              result = conn.brpoplpush(queue, working_queue, Sidekiq.options[:fetch_timeout] || 1)
              if result
                work = UnitOfWork.new(queue, result, working_queue)
              end
            end
          end
        end
        if work.nil?
          # If we get here, it's because there are no queues to process.
          # We can wind up with no queues to process if all queues
          # have been paused.  In that case, we don't want to enter into an infinite
          # busy loop so we'll sleep.
          sleep(1)
        end

        # Do not explicitly return, or will indicate to the ConnectionPool that the connection was interrupted and
        # disconnect you from Redis
        work
      }

      UnitOfWork = Struct.new(:queue, :job, :local_queue) do
        def acknowledge
          count = Sidekiq.redis {|conn| conn.lrem(local_queue, -1, job) }
          if count != 1
            Sidekiq.logger.error { "Unable to remove job from private queue #{local_queue}: #{count}" }
          end
          count
        end

        def queue_name
          queue.sub(/.*queue:/, '')
        end

        def requeue
          Sidekiq::Pro::Scripting.call(:super_requeue, [local_queue, queue], [job])
        end
      end

    end
  end
end

Sidekiq.configure_server do |config|
  config.on(:startup) do
    opts = Sidekiq.options
    if opts[:fetch] == Sidekiq::Pro::SuperFetch
      s = Sidekiq::Pro::SuperFetch::Retriever.new
      s.listen_for_pauses
      s.start(opts)
      Sidekiq::Pro::SuperFetch::Retriever.instance = s
    end
  end
end
