require 'set'
require 'socket'

module Sidekiq
  module Limiter
    class Bucket
      INTERVALS = Set.new([:second, :minute, :hour, :day])

      attr_reader :name, :interval, :key, :count

      # Create a rate limiter of +count+ per +interval+, e.g. 5 per second.
      #
      # Resets at the start of each interval so it's possible to perform
      # more than +count+ operations in a given interval, e.g. you can perform 5
      # operations at 12:03:58.999 and then another 5 operations at 12:03:59.001
      # since the interval resets at 12:03:59.000.
      #
      # Name should be a URL-safe descriptor, i.e. "user_12345", not "Bob's Rate".
      #
      def initialize(name, count, interval, options)
        @name = name
        @count = count
        raise ArgumentError, "Invalid name '#{name}', should be named like a Ruby variable: my_limiter" if name !~ LEGAL_NAME
        raise ArgumentError, "Unknown interval: #{interval}, should be one of #{INTERVALS.to_a}" if !INTERVALS.include?(interval)
        @interval = interval.to_sym
        @wait_for = (options[:wait_timeout] || 5).to_f
        @policy = options[:policy] || :raise
        @ttl = (options[:ttl] || Sidekiq::Limiter::DEFAULT_TTL).to_i
        @key = "lmtr-b-#{name}"

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
      # Yield if the current interval has not gone over limit.
      #
      # If this is a :second-based limiter, will +sleep+ up to
      # +wait_timeout+ seconds until it can fit within limit or
      # raise Sidekiq::Limiter::OverLimit.
      def within_limit
        start = Time.now.to_f
        loop do
          bkt = bucket_name

          ops, _ = Sidekiq::Limiter.redis do |conn|
            conn.pipelined do
              conn.incr(bkt)
              conn.expire(bkt, data_ttl)
            end
          end

          # We're within the limit for this bucket
          return yield if ops <= @count

          # if we can't wait anymore or this is a longer interval, just raise
          if Time.now.to_f > (start + @wait_for) || @interval != :second
            if @policy == :raise
              raise OverLimit, self
            else
              break
            end
          else
            # if we're on the :second policy, sleep until the next bucket
            t = Time.now.to_f
            pause(t.ceil - t)
          end
        end

        nil
      end

      private

      def pause(length)
        Sidekiq.logger.info { "Bucket rate limit #{name} exceeded, pausing..." }
        sleep(length)
      end

      # How long the bucket data will persist.
      # Note that we store all bucket data for one time
      # period larger than the interval.  If this is a
      # :second bucket, we'll store 60 buckets of data for
      # a minute of data.  If this is an :hour bucket, we'll
      # store 24 buckets for a day of data.
      #
      # This gives us recent usage history that we can display
      # in the Web UI without having to worry about truly
      # long term storage.
      def data_ttl
        @data_ttl ||= case @interval
                    when :second; 60
                    when :minute; 60 * 60
                    when :hour; 24 * 60 * 60
                    when :day; 30 * 24 * 60 * 60
                    end
      end

      def bucket_name
        bucket = case @interval
                 when :second; Time.now.strftime("%M:%S".freeze)
                 when :minute; Time.now.strftime("%H:%M".freeze)
                 when :hour; Time.now.strftime("%F:%H".freeze)
                 when :day; Time.now.strftime("%F".freeze)
                 else raise ArgumentError, "Unknown interval: #{@interval}"
                 end

        "lmtr-bdata-#{@name}-#{bucket}"
      end
    end

  end
end
