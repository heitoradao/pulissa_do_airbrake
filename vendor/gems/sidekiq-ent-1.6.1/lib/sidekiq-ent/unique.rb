require 'digest/sha1'


module Sidekiq
  module Enterprise
    ##
    # The Unique middleware adds a check before the push of a Sidekiq
    # job to Redis to see if the same job is already thought to be within Redis.
    #
    # This sets a tag in Redis which expires in N seconds. The same job cannot
    # be enqueued while this tag exists.
    #
    # When `perform` returns successfully we clear the tag so another identical
    # job can be enqueued at that point.  A raised error does **not** clear out
    # the tag so the same job cannot be pushed while the errored job is pending retry.
    #
    # If you are scheduling a unique job to run in the future, the uniqueness
    # will last until after the job is scheduled to run:
    #
    #     class MyUniqueWorker
    #       include Sidekiq::Worker
    #       sidekiq_options unique_for: 5 minutes
    #     end
    #     MyUniqueWorker.perform_in(1.hour, 1, 2, 3)
    #
    # Other duplicate jobs will be ignored for 65 minutes or until the job runs
    # successfully (in 60 minutes).
    #
    # You can "force push" a job to Redis by overriding `unique_for` to false:
    #
    #     MyUniqueWorker.set(unique_for: false).perform_async(1,2,3)
    #
    # There are several caveats with this feature and ways in which it can fail.
    # Do not depend on it for guaranteed uniqueness but rather as a way to prune
    # redundant jobs.  Your jobs should still be idempotent.
    #
    # Caveats:
    #
    # 1. Only works with simple parameters, no symbols, objects, etc, as the parameters
    #    must go thru the JSON serialization round trip without modification.
    # 2. If the job raises an error and does not retry, the tag will remain in Redis
    #    until it expires.  If you don't want jobs to retry, you should also set their
    #    uniqueness period very short.
    #
    # Usage:
    #
    # In your initializer:
    #
    #     Sidekiq::Enterprise.unique! unless Rails.env.test?
    #
    # In your worker:
    #
    #     sidekiq_options unique_for: 20.minutes
    #
    module Unique
      UNIQUE_KEY = 'unique_for'.freeze
      UNIQUE_UNTIL = 'unique_until'.freeze
      LOCKED_KEY = 'unlocks_at'.freeze
      TOKEN_KEY = 'unique_token'.freeze

      # Check to see if the unique lock is currently present
      # for the given (queue, klass, args) tuple.  Keep in mind that
      # the args must be *exactly* what would be deserialized
      # from JSON so no symbols, objects or non-JSON datatypes.
      #
      # Note this method IS RACY.  It can return false and then another
      # thread take the lock microseconds later - treat it as advisory only.
      #
      # Returns truthy if the unique lock is present.
      #
      def self.locked?(queue=nil, klass, args)
        queue ||= begin
          klass.is_a?(Sidekiq::Worker) ? klass.get_sidekiq_option['queue'] : 'default'
        end

        ctx = [klass, queue, args].join("|")
        hash = Digest::SHA1.hexdigest(ctx)
        Sidekiq.redis do |conn|
          conn.get("unique:#{hash}")
        end
      end

      class Client
        def call(worker, job, queue, redis_pool)
          if job[UNIQUE_KEY] && !job.has_key?(LOCKED_KEY)
            expiry = job[UNIQUE_KEY].to_i
            if expiry >= 0
              now = Time.now.to_f
              at = job['at'.freeze] || now
              ttl = (at + expiry)

              ctx = [job['class'.freeze], queue, job['args'.freeze]].join("|".freeze)
              hash = Digest::SHA1.hexdigest(ctx)
              result = redis_pool.with do |conn|
                conn.set("unique:#{hash}", ttl.to_s, nx: true, px: ((ttl - now) * 1000).to_i)
              end

              if result
                job[TOKEN_KEY] = hash.to_s
                job[LOCKED_KEY] = ttl.to_s
                return yield
              else
                Sidekiq.logger.info { "Skipping enqueue for #{job['class'.freeze]}, not unique." }
                return false
              end
            end
          end

          yield
        end
      end

      class Server
        def call(worker, job, queue)
          if job[UNIQUE_KEY] && job[UNIQUE_KEY].to_i >= 0
            policy = job[UNIQUE_UNTIL] || 'success'.freeze

            if policy == 'start'.freeze
              hash = job[TOKEN_KEY]
              timestamp = job[LOCKED_KEY]
              # Only the job which locked the hash can unlock the hash, since only it
              # should have this exact timestamp
              Sidekiq::Enterprise::Scripting.call(:unique_unlock, ["unique:#{hash}"], [timestamp])
            end

            yield

            if policy != 'start'.freeze
              hash = job[TOKEN_KEY]
              timestamp = job[LOCKED_KEY]

              # Only the job which locked the hash can unlock the hash, since only it
              # should have this exact timestamp
              Sidekiq::Enterprise::Scripting.call(:unique_unlock, ["unique:#{hash}"], [timestamp])
            end
          else
            yield
          end
        end

        Sidekiq::Enterprise::Scripting::LUA[:unique_unlock] = <<-LUA
          if redis.call('get', KEYS[1]) == ARGV[1] then
            redis.call('del', KEYS[1])
          end
        LUA
      end

    end

    def self.unique!
      Sidekiq.configure_client do |config|
        config.client_middleware do |chain|
          chain.add Sidekiq::Enterprise::Unique::Client
        end
      end

      Sidekiq.configure_server do |config|
        config.server_middleware do |chain|
          chain.insert_after Sidekiq::Limiter::Middleware, Sidekiq::Enterprise::Unique::Server
        end
        config.client_middleware do |chain|
          chain.add Sidekiq::Enterprise::Unique::Client
        end
      end
    end
  end
end
