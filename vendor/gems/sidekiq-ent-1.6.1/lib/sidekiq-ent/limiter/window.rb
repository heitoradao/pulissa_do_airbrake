require 'set'
require 'socket'

module Sidekiq
  module Limiter
    class Window
      INTERVALS = Set.new([:second, :minute, :hour, :day])

      attr_reader :name, :interval, :key, :count

      # Create a rate limiter of +count+ per +interval+, e.g. 5 per second.
      # +interval+ may be a symbol (:second, :minute) or an integer number
      # of seconds.
      #
      # This implements a "sliding window" limiter so you cannot perform
      # more than N operations until a full interval has passed.
      #
      # Name should be a URL-safe descriptor, i.e. "user_12345", not "Bob's Rate".
      #
      # Options:
      #   :wait_for - maximum time to pause while waiting for an available bucket, only applicable
      #               to :second buckets only.
      #
      def initialize(name, count, interval, options)
        @name = name
        @count = count
        raise ArgumentError, "Invalid name '#{name}', should be named like a Ruby variable: my_limiter" if name !~ LEGAL_NAME

        @interval = case interval
                    when Numeric
                      interval.to_i
                    when Symbol
                      case interval
                      when :second; 1
                      when :minute; 60
                      when :hour; 60 * 60
                      when :day; 24 * 60 * 60
                      else
                        raise ArgumentError, "Unknown interval: #{interval}, should be one of #{INTERVALS.to_a}" unless INTERVALS.include?(interval)
                      end
                    else
                      raise ArgumentError, "Unknown interval: #{interval.inspect}, should be one of #{INTERVALS.to_a} or a integer number of seconds"
                    end
        @wait_for = (options[:wait_timeout] || 5).to_f
        @policy = options[:policy] || :raise
        @key = "lmtr-w-#{name}"
        @ttl = (options[:ttl] || Sidekiq::Limiter::DEFAULT_TTL).to_i
        @bucket = "lmtr-wdata-#{@name}-#{@interval}"

        Sidekiq::Limiter.redis do |conn|
          conn.pipelined do
            conn.hmset(@key, "name", name, "size", @count, 'interval', @interval)
            conn.expire(@key, @ttl)
          end
        end
      end

      def to_s
        name
      end

      def status
        Sidekiq::Limiter::Status.new(@key)
      end

      ##
      # Yield if the current interval has not gone over quota.
      #
      # If this is a :second-based limiter, will +sleep+ up to
      # +wait_timeout+ seconds until it can fit within quota or
      # raise Sidekiq::Limiter::OverLimit.
      def within_limit
        start = Time.now.to_f
        loop do
          _, ops, ttl = Sidekiq::Limiter.redis do |conn|
            conn.multi do
              conn.set(@bucket, 0, ex: @interval, nx: true)
              conn.incr(@bucket)
              conn.ttl(@bucket)
            end
          end

          # deal with set/incr race condition
          # http://www.bennadel.com/blog/2785-volatile-keys-can-expire-mid-multi-transaction-in-redis-jedis.htm
          if ttl == -1
            Sidekiq::Limiter.redis do |conn|
              conn.expire(@bucket, @interval)
            end
          end

          # We're within the limit for this bucket
          return yield if ops <= @count

          # if we can't wait anymore or this is a longer interval, just raise
          if Time.now.to_f > (start + @wait_for) || @interval > @wait_for
            if @policy == :raise
              raise OverLimit, self
            else
              break
            end
          else
            # if we're on a small window policy, sleep a half second since we
            # don't know when the sliding window will reset.
            pause(0.5)
          end
        end

        nil
      end

      private

      def pause(length)
        Sidekiq.logger.info { "Window rate limit #{name} exceeded, pausing..." }
        sleep(length)
      end
    end

  end
end
