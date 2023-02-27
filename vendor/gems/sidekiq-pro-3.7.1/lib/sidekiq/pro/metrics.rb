require 'connection_pool'

module Sidekiq
  module Pro

    ##
    # Track useful metrics within Sidekiq Pro and Sidekiq Enterprise.
    # Set to something that quacks like a ::Datadog::Statsd object from the dogstatsd-ruby gem.
    #
    # Datadog::Statsd is a big improvement over basic Statsd, it is recommended.
    # Configure it in your initializer like this:
    #
    #   Sidekiq::Pro.dogstatsd = ->{ ::Datadog::Statsd.new("metrics.acmecorp.com", 8125) }
    #
    def self.dogstatsd=(thing)
      # statsd and dogstatsd are just different enough in API that we have to provide
      # an abstraction layer for both.  annoying but c'est la vie.
      if thing
        Sidekiq::Pro.metrics = Sidekiq::Pro::Metrics::Dogstatsd.new(thing)
      else
        Sidekiq::Pro.metrics = Sidekiq::Pro::Metrics::Nil.new
      end
    end

    ##
    # Track useful metrics within Sidekiq Pro and Sidekiq Enterprise.
    # Set to something that quacks like a ::Statsd object from the statsd-ruby gem.
    #
    # It is HIGHLY recommended that you use an IP address for the host or a localhost DNS cache.
    # Otherwise tons of DNS lookups can flood your network.
    #
    #   Sidekiq::Pro.statsd = ->{ ::Statsd.new("127.0.0.1") }
    #
    # PLEASE NOTE: Dogstatsd above has many more features (e.g. tags, events) and is the
    # recommended Statsd API. It does not require you to use the Datadog service, many
    # systems that understand the Statsd protocol support these extensions.
    #
    def self.statsd=(thing)
      if thing
        Sidekiq::Pro.metrics = Sidekiq::Pro::Metrics::Statsd.new(thing)
      else
        Sidekiq::Pro.metrics = Sidekiq::Pro::Metrics::Nil.new
      end
    end

    def self.metrics=(thing)
      @metrics = thing
    end

    def self.metrics(&block)
      if block
        yield @metrics if @metrics.enabled?
      else
        @metrics
      end
    end

    module Metrics

      ##
      # Support for the "classic" Statsd API in the statsd-ruby gem.
      class Statsd
        attr_reader :statsd
        def initialize(statsd)
          @statsd = if statsd.is_a?(::Proc)
            ::ConnectionPool.new(size: Sidekiq.options[:concurrency] + 2, &statsd)
          elsif statsd.is_a?(::ConnectionPool)
            statsd
          elsif statsd
            Sidekiq.logger.warn("Sidekiq::Pro.statsd should be set to a Proc, e.g. ->{ ::Statsd.new('127.0.0.1', 8125) }")
            ConnectionPool.new(size: 1) { statsd }
          end
        end

        def increment(name, opts=nil)
          @statsd.with{|c| c.increment(name) }
        end

        def decrement(name, opts=nil)
          @statsd.with{|c| c.decrement(name) }
        end

        def gauge(name, value, opts=nil)
          @statsd.with{|c| c.gauge(name, value) }
        end

        def histogram(name, value, opts=nil)
          # not supported
        end

        def count(name, value, opts=nil)
          @statsd.with{|c| c.count(name, value) }
        end

        def set(name, value, opts=nil)
          @statsd.with{|c| c.set(name, value) }
        end

        def timing(name, value, opts=nil)
          @statsd.with{|c| c.timing(name, value) }
        end

        def time(name, opts=nil, &block)
          @statsd.with{|c| c.time(name, &block) }
        end

        def batch(&block)
          @statsd.with{|c| c.batch(&block) }
        end
        def enabled?; true; end
      end

      ##
      # Support for the improved Statsd API published by Datadog in the dogstatsd-ruby gem.
      class Dogstatsd
        attr_reader :statsd
        def initialize(statsd)
          @statsd = if statsd.is_a?(::Proc)
            ::ConnectionPool.new(size: Sidekiq.options[:concurrency] + 2, &statsd)
          elsif statsd.is_a?(::ConnectionPool)
            statsd
          elsif statsd
            Sidekiq.logger.warn("Sidekiq::Pro.dogstatsd should be set to a Proc, e.g. ->{ Datadog::Statsd.new('127.0.0.1', 8125) }")
            ConnectionPool.new(size: 1) { statsd }
          end
        end

        def increment(name, opts={})
          @statsd.with{|c| c.increment(name, opts) }
        end

        def decrement(name, opts={})
          @statsd.with{|c| c.decrement(name, opts) }
        end

        def gauge(name, value, opts={})
          @statsd.with{|c| c.gauge(name, value, opts) }
        end

        def count(name, value, opts={})
          @statsd.with{|c| c.count(name, value, opts) }
        end

        def histogram(name, value, opts={})
          @statsd.with{|c| c.histogram(name, value, opts) }
        end

        def timing(name, value, opts={})
          @statsd.with{|c| c.timing(name, value, opts) }
        end

        def set(name, value, opts={})
          @statsd.with{|c| c.set(name, value, opts) }
        end

        def time(name, opts={}, &block)
          @statsd.with{|c| c.time(name, opts, &block) }
        end

        def batch(&block)
          @statsd.with{|c| c.batch(&block) }
        end
        def enabled?; true; end
      end

      ##
      # Support for no metrics
      class Nil
        def enabled?; false; end
        def decrement(name, opts=nil)
        end
        def increment(name, opts=nil)
        end
        def gauge(name, value, opts=nil)
        end
        def count(name, value, opts=nil)
        end
        def set(name, value, opts=nil)
        end
        def histogram(name, value, opts=nil)
        end
        def timing(name, value, opts=nil)
        end
        def time(name, opts=nil, &block)
          block.call
        end
        def batch(&block)
          block.call(self)
        end
      end

    end

    Sidekiq::Pro.metrics = Sidekiq::Pro::Metrics::Nil.new
  end
end
