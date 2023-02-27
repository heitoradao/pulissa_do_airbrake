module Sidekiq
  module Enterprise
    module Scripting

      LUA = {}
      SHAs = {}

      def self.bootstrap(pool=Sidekiq.redis_pool)
        Sidekiq.logger.debug { "Loading Sidekiq Enterprise Lua extensions" }

        pool.with do |c|
          LUA.each_with_object(SHAs) do |(name, lua), memo|
            memo[name] = c.script(:load, lua)
          end
        end
      end

      def self.call(name, keys, args, pool=Sidekiq.redis_pool)
        x = false
        bootstrap(pool) if SHAs.length != LUA.length

        pool.with do |c|
          c.evalsha(SHAs[name], keys, args)
        end

      rescue Redis::CommandError => ex
        if ex.message =~ /NOSCRIPT/
          # scripts got flushed somehow?
          if x
            raise
          else
            x = true
            bootstrap(pool)
            retry
          end
        else
          raise
        end
      end

    end
  end
end
