module Sidekiq
  module Middleware
    module Server

      ##
      # Send Sidekiq job metrics to a statsd server.
      #
      # Stats are namespaced by Worker class name:
      #
      #   jobs.WorkerClassName.count (counter)
      #   jobs.WorkerClassName.success (counter)
      #   jobs.WorkerClassName.failure (counter)
      #   jobs.WorkerClassName.perform (time gauge)
      #
      # Also sets global counters for tracking total job counts:
      #
      #   jobs.count
      #   jobs.success
      #   jobs.failure
      #
      # To configure the Statsd connection, you set the global:
      #
      #   Sidekiq::Pro.dogstatsd = ->{ Datadog::Statsd.new("hostname", 8125) }
      #
      class Statsd
        def initialize(options={})
          # DEPRECATED options[:client] is no longer recommended
          # TODO Remove in Pro 4.0
          @statsd = options[:client] || Sidekiq::Pro.metrics
        end

        def call(worker, msg, queue, &block)
          w = msg['wrapped'.freeze] || worker.class.to_s
          begin
            @statsd.batch do |b|
              b.increment("jobs.count")
              b.increment("jobs.#{w}.count")
            end
            @statsd.batch do |b|
              b.time("jobs.#{w}.perform", &block)
              b.increment("jobs.success")
              b.increment("jobs.#{w}.success")
            end
          rescue Exception
            @statsd.batch do |b|
              b.increment("jobs.failure")
              b.increment("jobs.#{w}.failure")
            end
            raise
          end
        end
      end
    end
  end
end
