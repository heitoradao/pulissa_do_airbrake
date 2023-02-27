require 'digest/sha1'
require 'sidekiq/util'

module Sidekiq
  module Periodic
    ##
    # We have N sidekiq processes starting up and one shared Redis.
    # We want to get a common view of the periodic jobs.  It is
    # assumed that all Sidekiqs are running the same codebase but
    # we do want to handle the case of old Sidekiqs which are
    # lingering around.
    #
    # To prevent processes from stepping on each other when updating
    # Redis, we elect a single Sidekiq process as leader to update
    # the periodic data model.
    #
    # Periodic data is versioned so that if any jobs are added, removed
    # or changed, the data model keys in Redis will change completely.  The
    # old version will quietly garbage collect when its TTL expires.
    #
    # Startup:
    # 1. register periodic jobs in-memory
    # 2. generate version SHA from periodic data
    # 3. elect leader for version
    # 4. leader pushes periodic data to Redis if version has changed
    #
    # Ongoing
    # 1. All processes run a Periodic Actor every minute:
    #   a. on the leader, this checks for new periodic jobs to create
    #   b. creates those jobs
    # 2. On all others, this checks if leader is still active.
    #
    class Manager
      include Sidekiq::Util

      SAVE_COUNT = 25

      def self.instance
        @mgr ||= Sidekiq::Periodic::Manager.new
      end

      def initialize
        @thread = nil
        @done = false
        @sleeper = ConnectionPool::TimedStack.new
      end

      def persist(config)
        @q = config.finish!

        if Sidekiq::Senate.leader?
          config.persist
          logger.info { "Managing #{@q.size} periodic jobs" }
        end
      end

      def start
        @thread ||= safe_thread("periodic", &method(:cycle))
      end

      def terminate
        @done = true
        @sleeper << nil
      end

      def cycle
        while !@done
          a = Time.now.min
          begin
            process
          rescue => ex
            handle_exception(ex)
          end
          b = Time.now.min
          Sidekiq.logger.warn { "Periodic subsystem skipped tick, is Redis slow?" } if a != b

          begin
            @sleeper.pop(seconds_until_next_minute)
          rescue Timeout::Error
          end
        end
        Sidekiq.logger.info { "Periodic subsystem stopped" }
      end

      def process(now=Time.now)
        results = [now]
        return results unless Sidekiq::Senate.leader?

        tries = 5
        lock = nil
        (cycle, timestamp) = job_due(now)
        while cycle && tries > 0
          begin
            unless lock
              lock = take_lock(timestamp.to_i)
              return (Sidekiq.logger.warn("Unable to take periodic lock!"); results) unless lock
            end

            enqueue_job(cycle, timestamp, now)
            results << cycle.lid
            (cycle, timestamp) = job_due(now)
            tries = 5
          rescue => ex
            handle_exception(ex)
            tries -= 1
            sleep 1
          end
        end

        logger.debug { "Tick: #{results}" }
        results
      end

      private

      def take_lock(seconds)
        Sidekiq.redis do |conn|
          conn.set("periodic:#{seconds}", identity, nx: true, ex: 60)
        end
      end

      def seconds_until_next_minute(now=Time.now.to_f)
        inow = now.to_i
        next_min = inow + (60 - (inow % 60))
        next_min - now
      end

      def job_due(now=Time.now)
        tstamp = @q.next_key
        return nil if !tstamp || now.to_i < tstamp

        cycle = @q.pop

        time = cycle.next_occurrence(now+1)
        Sidekiq.logger.debug { "Now: #{now+1}, Next: #{Time.at(time)}" }
        # push next occurrence back onto heap
        @q.push(time, cycle)

        [cycle, tstamp]
      end

      def enqueue_job(cycle, ts, now=Time.now)
        # Create new job
        defs = {'class' => cycle.klass, 'args' => []}
        klass = cycle.klass
        if klass.respond_to?(:get_sidekiq_options)
          defs = defs.merge(klass.get_sidekiq_options)
        end

        jid = Sidekiq::Client.push(defs.merge(cycle.job_hash))
        if jid
          Sidekiq.redis do |conn|
            conn.pipelined do
              # push record of job to periodic history
              zset = "loop-history-#{cycle.lid}"
              conn.zadd(zset, ts.to_s, jid)
              # prune all but the latest 25
              conn.zremrangebyrank(zset, 0, (SAVE_COUNT * -1) - 1)
              conn.expire(zset, Sidekiq::Periodic::Config::STATIC_TTL)
            end
          end
          logger.info { "Enqueued periodic job #{cycle.klass} with JID #{jid} for #{Time.at(ts)}" }
        else
          logger.info { "Periodic job #{cycle.klass} did not push to Redis" }
        end

        jid
      end
    end
  end
end
