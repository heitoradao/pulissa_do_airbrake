require 'sidekiq/pro/config'

module Sidekiq

  ##
  # The Pending set is where Sidekiq puts jobs that are being processed
  # at the moment.  They stay in the zset until they are acknowledged as
  # finished or they time out.
  class PendingSet < JobSet
    def initialize
      super 'pending'
    end

    # Iterate through the pending set, pushing any jobs which have timed out
    # back to their original queue.  This should iterate oldest first so it won't
    # pull over every single element, just ones which have timed out.
    def pushback
      @last = Time.now.to_f

      count = 0
      now = Time.now.to_f
      each do |job|
        if job.score < now
          res = Sidekiq::Pro::Scripting.call(:timed_requeue, [Sidekiq::Pro::TimedFetch::PENDING, "queues", "queue:#{job.queue}"], [job.value, job.queue])
          count += 1 if res
        end
      end
      count
    end

    # This will call pushback every TIMEOUT seconds, ensuring that lingering
    # jobs are pushed back and rerun.
    #
    # Returns [int] number of jobs pushed back or [nil] if not time yet
    def limited_pushback
      return if Time.now.to_f < @last + Sidekiq::Pro::TimedFetch::TIMEOUT
      pushback
    end

    # This method allows you to destroy a pending job which is constantly failing
    # and/or crashing the process.  All you need is the JID.
    #
    #    Sidekiq::PendingSet.new.destroy(jid)
    #
    def destroy(jid)
      entry = find_job(jid)
      delete_by_jid(entry.score, entry.jid) if entry
    end
  end

  module Pro

    ##
    # Provides reliable queue processing within Redis using Lua.
    #
    # 1. Pull a job from a queue and push it onto a zset scored on job timeout.
    # 2. Process the job
    # 3. Acknowledge the work by removing it from the zset
    #
    # If we crash during this process, upon restart we'll move any jobs which have timed out
    # back onto their respective queues, effectively recovering the jobs that
    # were processing during the crash.  This also means if a job is crashing Sidekiq, it won't
    # be reprocessed for an hour, avoiding the dreaded "poison pill" wherein a job could
    # crash all Sidekiq processes if we try to re-process it immediately.
    #
    # NB: this reliable algorithm does not require stable hostnames or unique indexes, unlike
    # ReliableFetch, so it will work on Heroku, in Docker or ECS.  It will autoscale with EBS.
    # It does not use private queues so you won't orphan jobs.
    #
    # The drawback is that it has to use O(log N) Redis operations so it will get slower as
    # more and more jobs are processed simultaneously. As always, monitor your load and Redis
    # latency, please.
    #
    class TimedFetch

      # Jobs have 1 hour to complete or they can be
      # pushed back onto the queue for re-processing.
      TIMEOUT = 3600
      PENDING = 'pending'.freeze

      def initialize(options)
      end

      def retrieve_work
        Manager.instance.retrieve_work
      end

      def self.bulk_requeue(in_progress, _=nil)
        in_progress.each(&:requeue)
      rescue => ex
        # best effort, ignore Redis network errors
        Sidekiq.logger.error { "Failed to bulk_requeue: #{ex.message}" }
      end

      class Manager
        attr_accessor :paused
        attr_accessor :sleeptime
        attr_accessor :timeout
        class << self
          attr_accessor :instance
        end

        def initialize(sleeptime=1, options)
          @options = options
          @sleeptime = sleeptime
          @paused = Set.new
          @timeout = TIMEOUT
          @queues = options[:queues].map {|q| "queue:#{q}" }
          @shuffle = !(options[:strict] && @queues.length == @queues.uniq.length)
          @ps = Sidekiq::PendingSet.new

          Sidekiq.logger.info("TimedFetch activated")
          Sidekiq.logger.error("DEPRECATED: TimedFetch is deprecated, switch to super_fetch.")

          count = @ps.pushback
          Sidekiq.logger.warn { "TimedFetch pushed back #{count} timed out jobs" } if count > 0

          listen_for_pauses
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

        def retrieve_work
          count = @ps.limited_pushback
          Sidekiq.logger.warn { "TimedFetch pushed back #{count} timed out jobs" } if count && count > 0

          pull(active_queues)
        end

        private unless $TESTING

        def active_queues
          if @changed
            @queues = (@options[:queues] - @paused.to_a).map {|q| "queue:#{q}" }
            @changed = nil
          end
          @queues
        end

        Sidekiq::Pro::Scripting::LUA_SCRIPTS[:timed_fetch] = <<-LUA
          local timeout = ARGV[1]
          local idx = 2
          local size = #KEYS
          while idx <= size do
            local queue = KEYS[idx]
            local jobstr = redis.call('rpop', queue)
            if jobstr then
              redis.call('zadd', KEYS[1], timeout, jobstr)
              return {queue, jobstr}
            end
            idx = idx + 1
          end
          return nil
        LUA

        def pull(queues)
          # In a weighted ordering, treat the queues like we're drawing
          # a queue out of a hat: draw a queue, attempt to fetch work.
          # Draw another queue, attempt to fetch work.
          queues = queues.shuffle if @shuffle

          limit = Time.now.to_f + timeout
          queue, job = nil

          if queues.size > 0
            keys = [PENDING]
            keys.concat(queues)
            args = [limit]

            queue, job = Sidekiq.redis do |conn|
              Sidekiq::Pro::Scripting.call(:timed_fetch, keys, args)
            end
          end

          if queue
            UnitOfWork.new(queue, job)
          else
            # If we get here, it's because there are no queues to process
            # or the queues are empty. We can wind up with no queues to
            # process if all queues have been paused.  In that case,
            # we don't want to enter into an infinite busy loop so we'll sleep.
            sleep(@sleeptime)
            nil
          end
        end

      end

      UnitOfWork = Struct.new(:queue, :job) do
        def acknowledge
          result = Sidekiq.redis {|conn| conn.zrem(Sidekiq::Pro::TimedFetch::PENDING, job) }
          Sidekiq.logger.error("Unable to remove job from pending set!") unless result
          result
        end

        def queue_name
          queue.sub(/.*queue:/, ''.freeze)
        end

        def requeue
          Sidekiq::Pro::Scripting.call(:timed_requeue, [Sidekiq::Pro::TimedFetch::PENDING, "queues".freeze, queue], [job, queue_name])
        end
      end

    end
  end
end
