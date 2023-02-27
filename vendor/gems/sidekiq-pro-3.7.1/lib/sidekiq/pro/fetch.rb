require 'concurrent'
require 'sidekiq/pro/config'

module Sidekiq::Pro
  ##
  # Provides reliable queue processing via Redis' rpoplpush command.
  #
  # 1. retrieve the work while pushing it to our private queue for this process.
  # 2. process the work
  # 3. acknowledge the work by removing it from our private queue
  #
  # If we crash during this process, upon restart we'll pull any existing work from
  # the private queue and work on that first, effectively recovering the jobs that
  # were processing during the crash.
  class ReliableFetch
    def initialize(retriever=Retriever.instance, options)
      raise ArgumentError, "reliable fetch requires a process index option" if !options[:index].is_a?(Integer)
      @retriever = retriever
    end

    def retrieve_work
      @retriever.retrieve_work
    end

    def self.bulk_requeue(in_progress, options)
      # Ignore the in_progress arg passed in; rpoplpush lets us know everything in process
      Sidekiq.redis do |conn|
        get_queues(options).each do |(queue, working_queue)|
          while conn.rpoplpush(working_queue, queue)
            Sidekiq.logger.info {"Moving job from #{working_queue} back to #{queue}"}
          end
        end
      end
    rescue => ex
      # best effort, ignore Redis network errors
      Sidekiq.logger.info { "Failed to requeue: #{ex.message}" }
    end

    def self.private_queue(q, options)
      if options[:ephemeral_hostname]
        # Running on Heroku, hostnames are not predictable or stable.
        "queue:#{q}_#{options[:index]}"
      else
        "queue:#{q}_#{Socket.gethostname}_#{options[:index]}"
      end
    end

    def self.get_queues(options)
      options[:queues].map {|q| ["queue:#{q}", private_queue(q, options)] }
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
        @queues = ReliableFetch.get_queues(@options)
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

      def startup
        watchdog("ReliableFetch#startup") do
          Sidekiq.logger.info("ReliableFetch activated")
          Sidekiq.logger.error("DEPRECATED: ReliableFetch is deprecated, switch to super_fetch.")

          # Heroku can get into a situation where the old and new process
          # are running concurrently.  Sleep 15 sec to ensure the old
          # process is dead before we take jobs from the internal queue.
          sleep(15) if ENV['DYNO']

          # Need to unique here or we duplicate jobs!
          # https://github.com/mperham/sidekiq/issues/2120
          queues = @queues.uniq
          bulk_reply = Sidekiq.redis do |conn|
            conn.pipelined do
              queues.each do |(_, working_queue)|
                conn.lrange(working_queue, 0, -1)
              end
            end
          end
          internals = []
          bulk_reply.each_with_index do |vals, i|
            queue = queues[i][0]
            working_queue = queues[i][1]
            xform = vals.map do |msg|
              [queue, working_queue, msg]
            end
            internals.unshift(*xform)
          end
          @internal = internals
          Sidekiq.logger.warn("ReliableFetch: recovering work on #{@internal.size} jobs") if @internal.size > 0
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

        if @internal.size > 0
          (queue, working_queue, msg) = @internal.pop
          Sidekiq.logger.warn("Processing recovered job from queue #{queue} (#{working_queue}): #{msg.inspect}")
          UnitOfWork.new(queue, msg, working_queue)
        else
          @algo.call(active_queues)
        end
      end

      private unless $TESTING

      def active_queues
        if @changed
          @queues = (@options[:queues] - @paused.to_a).map {|q| ["queue:#{q}", ReliableFetch.private_queue(q, @options)] }
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
          result = Sidekiq.redis {|conn| conn.lrem(local_queue, -1, job) }
          if result != 1
            Sidekiq.logger.error("Unable to remove job from private queue!")
          end
          result
        end

        def queue_name
          queue.sub(/.*queue:/, '')
        end

        def requeue
          # no worries, mate, rpoplpush got our back!
        end
      end

    end
  end
end

Sidekiq.configure_server do |config|
  config.on(:startup) do
    if Sidekiq.options[:fetch] == Sidekiq::Pro::ReliableFetch
      s = Sidekiq::Pro::ReliableFetch::Retriever.new
      s.listen_for_pauses
      s.start(Sidekiq.options)
      Sidekiq::Pro::ReliableFetch::Retriever.instance = s
    end
  end
end
