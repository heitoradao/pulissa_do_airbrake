require 'socket'
require 'sidekiq'

module Sidekiq
  module Limiter

    ##
    # A concurrent limiter allows a block to execute iff it can
    # obtain a token. It tracks the following metrics:
    #
    # - Number of times a thread got a token immediately
    # - Number of times a thread had to wait for a token
    # - Number of times a thread could not get a token.
    # - Number of times a thread held a token for more than the locktime
    # - Total wait time to get a token
    # - Total held time for a token
    #
    # +name+ should be a URL-safe descriptor, i.e. "user_12345", not "Bob's Rate".
    # +policy+ can be :raise (default) to raise OverLimit if a token cannot be obtained within +wait_timeout+,
    #   or :ignore if the block should be skipped if the rate limit can't be fulfilled.
    #
    class Concurrent
      attr_reader :name, :size, :key

      def initialize(name, size, options)
        @name = name
        raise ArgumentError, "Invalid name '#{name}', should be named like a Ruby variable: my_limiter" if name !~ LEGAL_NAME
        @size = size
        @lock_for = (options[:lock_timeout] || 30).to_i
        @wait_for = (options[:wait_timeout] || 5).to_f
        raise ArgumentError, "wait_timeout must be an integer value" if @wait_for < 1 && @wait_for != 0
        @policy = options[:policy] || :raise
        @ttl = (options[:ttl] || Sidekiq::Limiter::DEFAULT_TTL).to_i
        @free = "lmtr-cfree-#{name}"
        @pend = "lmtr-cpend-#{name}"
        @used = "lmtr-cused-#{name}"
        @key = "lmtr-c-#{name}"

        Sidekiq::Enterprise::Scripting.call(:limiter_concurrent_build,
                                     [@free, @pend, @used, @key],
                                     [@size, Time.now.to_f.to_s, @name, @ttl],
                                     Sidekiq::Limiter.redis_pool)
      end

      def to_s
        name
      end

      def status
        Sidekiq::Limiter::Status.new(@key)
      end

      def within_limit
        lck = nil
        begin
          lck = lock
          return unless lck # :ignore policy
          holdstart = Time.now
          yield
        rescue Sidekiq::Limiter::OverLimit => ex
          Sidekiq::Limiter.redis do |conn|
            conn.hincrby(@key, "overrated", 1)
          end if ex.limiter == self
          raise
        ensure
          unlock(lck, holdstart) if lck
        end
      end

      private

      Sidekiq::Enterprise::Scripting::LUA[:limiter_concurrent_build] = <<-LUA
        local rc = redis.call("exists", KEYS[4])
        if rc == 1 then
          local lsz = redis.call("llen", KEYS[1])
          local psz = redis.call("llen", KEYS[2])
          local zsz = redis.call("zcard", KEYS[3])
          if not (lsz + zsz + psz == tonumber(ARGV[1])) then
            redis.log(redis.LOG_WARNING, "[sidekiq] Concurrent limiter '" .. ARGV[3] .. "' changed size from " .. (lsz + zsz + psz) .. " to " .. ARGV[1] .. ", rebuilding...")
            redis.call("del", KEYS[1], KEYS[2], KEYS[3], KEYS[4])
          else
            -- fast exit if keys already exist

            -- push one lock from pending back to free, just in case one leaked
            redis.call("rpoplpush", KEYS[2], KEYS[1])

            redis.call("expire", KEYS[1], ARGV[4])
            redis.call("expire", KEYS[2], ARGV[4])
            redis.call("expire", KEYS[3], ARGV[4])
            redis.call("expire", KEYS[4], ARGV[4])
            return 0
          end
        end

        redis.log(redis.LOG_VERBOSE, "Creating new concurrent limiter " .. ARGV[3])
        local i = tonumber(ARGV[1])
        while i > 0 do
          redis.call("lpush", KEYS[1], i .. ARGV[2])
          i = i - 1
        end
        redis.call("expire", KEYS[1], ARGV[4])

        redis.call("hset", KEYS[4], "name", ARGV[3])
        redis.call("expire", KEYS[4], ARGV[4])
        return 1
      LUA

      Sidekiq::Enterprise::Scripting::LUA[:limiter_concurrent_unlock] = <<-LUA
        local count = redis.call("zrem", KEYS[2], ARGV[1])
        if count == 0 then
          -- reclaimed?
          redis.log(redis.LOG_VERBOSE, "[sidekiq] No such token " .. ARGV[1] .. " in " .. KEYS[2] .. ", likely reclaimed...")
        else
          redis.call("lpush", KEYS[1], ARGV[1])
          redis.call("expire", KEYS[1], ARGV[5])
        end

        redis.call('hincrby', KEYS[3], "held", 1)
        redis.call('hincrby', KEYS[3], "held_ms", ARGV[3])

        local wait = tonumber(ARGV[2])
        if wait == 0 then
          redis.call('hincrby', KEYS[3], "immediate", 1)
        else
          redis.call('hincrby', KEYS[3], "wait_ms", wait)
          redis.call('hincrby', KEYS[3], "waited", 1)
        end

        if ARGV[4] == "true" then
          redis.call('hincrby', KEYS[3], "overtime", 1)
        end

        return count
      LUA

      def unlock(lockdata, holdstart)
        endt = Time.now
        held_ms = ((endt - holdstart).to_f * 1000).round
        (_, token, wait) = lockdata
        wait_ms = (wait.to_f * 1000).round
        overage = endt - holdstart > @lock_for

        Sidekiq::Enterprise::Scripting.call(:limiter_concurrent_unlock,
                                     [@free, @used, @key],
                                     [token, wait_ms, held_ms, overage, @ttl],
                                     Sidekiq::Limiter.redis_pool)
      end

      Sidekiq::Enterprise::Scripting::LUA[:limiter_concurrent_lock] = <<-LUA
        local token = redis.call("lpop", KEYS[1])
        if not token then
          local count = redis.call("zremrangebyscore", KEYS[2], "-inf", ARGV[2])
          if count == 0 then
            return nil
          end

          redis.log(redis.LOG_WARNING, "[sidekiq] Reclaiming " .. count .. " expired locks for " .. KEYS[1])
          redis.call('hincrby', KEYS[3], "reclaimed", count)
          while count > 0 do
            redis.call('lpush', KEYS[1], count .. ARGV[2])
            count = count - 1
          end
          redis.call("expire", KEYS[1], ARGV[3])

          -- grab one of the reclaimed tokens for ourself immediately
          local token = redis.call("lpop", KEYS[1])
          redis.call("zadd", KEYS[2], ARGV[1], token)
          return token
        else
          redis.call("zadd", KEYS[2], ARGV[1], token)
          return token
        end
      LUA

      def lock
        start = Time.now
        expiry, token = nil

        t = Time.now.to_f
        expiry = t + @lock_for
        token = Sidekiq::Enterprise::Scripting.call(:limiter_concurrent_lock,
                                             [@free, @used, @key],
                                             [expiry.to_s, t.to_s, @ttl],
                                             Sidekiq::Limiter.redis_pool)
        return [expiry, token] if token

        Sidekiq::Limiter.redis do |conn|
          wait_until = start + @wait_for
          diff = wait_until - Time.now
          while diff > 0
            # Wait for a token to reappear in the free list
            candidate = conn.brpoplpush(@free, @pend, diff.to_f.ceil.to_i)

            if candidate
              t = Time.now.to_f
              expiry = t + @lock_for
              token = Sidekiq::Enterprise::Scripting.call(:limiter_concurrent_lock,
                                                   [@pend, @used, @key],
                                                   [expiry.to_s, t.to_s],
                                                   Sidekiq::Limiter.redis_pool)
              return [expiry, token, Time.now - start] if token
            end
            diff = wait_until - Time.now
          end

          raise OverLimit, self if @policy == :raise
        end

        nil
      end
    end

  end
end
