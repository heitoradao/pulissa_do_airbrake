# Require in your initializer:
#
#   require 'sidekiq/pro/expiry'
#
# Use like:
#
#   class MyWorker
#     sidekiq_options expires_in: 30.minutes
#
module Sidekiq::Middleware::Expiry

  class Client
    def call(worker, msg, queue, redis_pool)
      if msg['expires_in'] && !msg['expires_at']
        ein = msg['expires_in'].to_f
        raise ArgumentError, "expires_in must be a relative time, not absolute time" if ein > 1_000_000_000
        msg['expires_at'] = (msg['at'] || Time.now.to_f) + ein
      end
      yield
    end
  end

  class Server
    def call(worker, msg, queue)
      if msg['expires_at'] && Time.now > Time.at(msg['expires_at'])
        Sidekiq::Pro.metrics {|m| m.increment("jobs.expired", tags: ["worker:#{msg['class']}", "queue:#{queue}"]) }
        return worker.logger.info("Expired job #{worker.jid}")
      end

      yield
    end
  end
end

Sidekiq.configure_client do |config|
  config.client_middleware do |chain|
    chain.add Sidekiq::Middleware::Expiry::Client
  end
end
Sidekiq.configure_server do |config|
  config.client_middleware do |chain|
    chain.add Sidekiq::Middleware::Expiry::Client
  end
  config.server_middleware do |chain|
    chain.add Sidekiq::Middleware::Expiry::Server
  end
end
