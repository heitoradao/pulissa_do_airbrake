require 'sidekiq-ent/periodic/cron'

module Sidekiq

  ##
  # A Loop generates jobs on some schedule, e.g. process new orders every 15 minutes.
  # Loops are registered on startup.
  #
  # Here's how to register a loop:
  #
  #   Sidekiq.configure_server do |config|
  #     config.periodic do |mgr|
  #       mgr.register "*/4 * * * * *", ProcessOrders, retry: 3
  #     end
  #   end
  #
  def self.periodic
    yield Sidekiq::Periodic::Config.instance if block_given?
  end

  module Periodic
    class LoopSet
      include Enumerable

      def initialize
        @lids = Sidekiq.redis do |conn|
          ver = conn.get("periodic-version")
          ver ? conn.smembers("loops-#{ver}") : []
        end
      end

      def size
        @lids.size
      end

      def each
        @lids.each do |lid|
          yield Loop.new(lid)
        end
      end
    end

    class Loop
      attr_reader :klass, :schedule, :lid

      def initialize(lid)
        @lid = lid
        @klass = options.delete('class')
        @schedule = options.delete('schedule')
      end

      def options
        @options ||= Sidekiq.redis do |c|
          c.hgetall("loop-#{lid}")
        end
      end

      def next_run
        Sidekiq::CronParser.new(@schedule).next(Time.now)
      end

      # returns [jid, timestamp] pairs for each execution
      def history
        Sidekiq.redis do |c|
          c.zrevrange("loop-history-#{lid}", 0, -1, :with_scores => true)
        end
      end
    end

  end
end

Sidekiq.configure_server do |config|
  require 'sidekiq-ent/periodic/config'
  require 'sidekiq-ent/periodic/manager'

  config.on(:leader) do
    cfg = Sidekiq::Periodic::Config.instance
    if !cfg.empty?
      act = Sidekiq::Periodic::Manager.instance
      act.persist(cfg)
      act.start
    else
      # issue #3322
      cfg.clear
    end
  end
  config.on(:shutdown) do
    act = Sidekiq::Periodic::Manager.instance
    act.terminate
  end
end
