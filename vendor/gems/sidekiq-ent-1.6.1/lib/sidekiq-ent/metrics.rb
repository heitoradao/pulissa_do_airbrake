require 'sidekiq/util'
require 'sidekiq/api'

module Sidekiq

  def self.save_history(statsd, interval=30, &block)
    instance = Sidekiq::Enterprise::History.new
    instance.statsd = statsd
    instance.interval = interval
    instance.custom = block

    on(:leader) do
      instance.start
    end
    on(:shutdown) do
      instance.stop
    end
  end

  module Enterprise
    class History
      include Sidekiq::Util
      attr_accessor :statsd
      attr_accessor :interval
      attr_accessor :custom

      def initialize
        @done = false
        @interval = 30
      end

      def start
        raise "statsd not configured!" unless statsd
        logger.info("Sending processing metrics to #{statsd.inspect}")
        @thread ||= safe_thread("history", &method(:run))
      end

      def stop
        @done = true
      end

      def run
        until @done
          begin
            capture if Sidekiq::Senate.leader?
          rescue => ex
            handle_exception(ex)
          end
          sleep(interval)
        end
      end

      def capture
        sidekiq_stats = Sidekiq::Stats.new
        capture_default(sidekiq_stats)

        if custom
          custom.call(statsd, sidekiq_stats)
        end
      end

      def capture_default(stats)
        statsd.batch do |s|
          s.gauge("sidekiq.failed", stats.failed)
          s.gauge("sidekiq.processed", stats.processed)
          s.gauge("sidekiq.enqueued", stats.enqueued)
          s.gauge("sidekiq.retries", stats.retry_size)
          s.gauge("sidekiq.dead", stats.dead_size)
          s.gauge("sidekiq.scheduled", stats.scheduled_size)
          s.gauge("sidekiq.busy", stats.workers_size)

          stats.queues.each_pair do |q, length|
            s.gauge("sidekiq.enqueued.#{q}", length)
          end
        end
      end

    end
  end

end

