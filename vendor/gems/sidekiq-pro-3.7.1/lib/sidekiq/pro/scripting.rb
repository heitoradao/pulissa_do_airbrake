module Sidekiq
  module Pro
    module Scripting

      LUA_SCRIPTS = {
        :timed_requeue => <<-LUA,
          local job = redis.call('zrem', KEYS[1], ARGV[1])
          if job == 1 then
            redis.call('sadd', KEYS[2], ARGV[2])
            redis.call('rpush', KEYS[3], ARGV[1])
            return true
          end

          return false
        LUA
        :super_requeue => <<-LUA,
          -- foo
          local val = redis.call('lrem', KEYS[1], -1, ARGV[1])
          if val == 1 then
            redis.call('lpush', KEYS[2], ARGV[1])
          end
        LUA
        :queue_delete_by_jid => <<-LUA,
          local window = 50
          local cursor = tonumber(ARGV[2])
          if cursor == -1 then
            cursor = redis.call('llen', KEYS[1])
          end
          cursor = cursor - window
          if cursor < 0 then
            cursor = 0
          end

          local idx = 0
          local result = nil
          local jobs = redis.call('lrange', KEYS[1], cursor, cursor+window-1)
          for _,jobstr in pairs(jobs) do
            if string.find(jobstr, ARGV[1]) then
              local job = cjson.decode(jobstr)
              if job.jid == ARGV[1] then
                redis.call('lrem', KEYS[1], 1, jobstr)
                result = jobstr
                break
              end
            end
          end
          if result then
            return result
          end
          if cursor == 0 then
            return nil
          else
            return cursor
          end
        LUA
        :fast_enqueue => <<-LUA,
          local queue = ARGV[2].."queue:"
          local jobs = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], "LIMIT", "0", "100")
          local count = 0
          local now = ARGV[1]
          for _,jobstr in pairs(jobs) do
            local job = cjson.decode(jobstr)

            -- Hideous hack to work around cjson's braindead large number handling
            -- https://github.com/mperham/sidekiq/issues/2478
            if job.enqueued_at == nil then
              jobstr = string.sub(jobstr, 1, string.len(jobstr)-1) .. ',"enqueued_at":' .. now .. '}'
            else
              jobstr = string.gsub(jobstr, '(\"enqueued_at\":)[1-9][0-9.]+', '%1' .. now)
            end

            redis.call('sadd', KEYS[2], job.queue)

            -- WARNING
            -- We don't know which queues we'll be pushing jobs to until
            -- we're actually executing so this script technically violates
            -- the Redis Cluster requirements for Lua since we can't pass in
            -- the full list of keys we'll be mutating.
            redis.call('lpush', queue..job.queue, jobstr)

            count = count + 1
          end
          if count > 0 then
            if count == 100 then
              redis.call('zrem', KEYS[1], unpack(jobs))
            else
              redis.call('zremrangebyscore', KEYS[1], '-inf', ARGV[1])
            end
          end
          return count
        LUA
      }

      SHAs = {}

      def self.bootstrap
        Sidekiq.logger.debug { "Loading Sidekiq Pro Lua extensions" }

        Sidekiq.redis do |conn|
          LUA_SCRIPTS.each_with_object(SHAs) do |(name, lua), memo|
            memo[name] = conn.script(:load, lua)
          end
        end
      end

      def self.call(name, keys, args)
        bootstrap if SHAs.length != LUA_SCRIPTS.length

        Sidekiq.redis do |conn|
          conn.evalsha(SHAs[name], keys, args)
        end

      rescue Redis::CommandError => ex
        if ex.message =~ /NOSCRIPT/
          # scripts got flushed somehow?
          bootstrap
          retry
        else
          raise
        end
      end

    end
  end
end
