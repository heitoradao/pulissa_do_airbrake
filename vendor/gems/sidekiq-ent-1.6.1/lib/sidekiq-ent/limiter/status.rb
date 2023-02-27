module Sidekiq

  class LimiterSet
    include Enumerable

    def each(&block)
      Sidekiq::Limiter.redis do |conn|
        conn.scan_each(:match => "lmtr-?-*".freeze, :count => 1000) do |key|
          yield Sidekiq::Limiter::Status.new(key)
        end
      end
    end

    def paginate(cursor, &block)
      nxt = 1
      results = []
      Sidekiq::Limiter.redis do |conn|
        while results.size < 25 && nxt != "0"
          (nxt, keys) = conn.scan(cursor, :match => "lmtr-?-*".freeze, :count => 200)
          keys.each do |key|
            results << Sidekiq::Limiter::Status.new(key)
          end
          cursor = nxt
        end
      end
      return [nxt, results]
    end

  end

  module Limiter
    class Status

      class Concurrent
        attr_accessor :used, :size, :available

        def initialize(key, name)
          @available, @used, @metrics = Sidekiq::Limiter.redis do |conn|
            conn.multi do
              conn.llen("lmtr-cfree-#{name}")
              conn.zcard("lmtr-cused-#{name}")
              conn.hgetall("lmtr-c-#{name}")
            end
          end
          @size = @available + @used
          @metrics.default = 0
        end

        def rate; @size; end
        def type; :concurrent; end
        def type_name; 'Concurrent'.freeze; end

        def available_pct
          size == 0 ? 0 : (@available.to_f / size) * 100
        end

        def used_pct
          size == 0 ? 0 : (@used.to_f / size) * 100
        end

        def held
          @metrics["held"]
        end

        def held_time
          @metrics["held_ms"].to_f / 1000
        end

        def immediate
          @metrics["immediate"]
        end

        def reclaimed
          @metrics["reclaimed"]
        end

        def waited
          @metrics["waited"]
        end

        def wait_time
          @metrics["wait_ms"].to_f / 1000
        end

        def overtime
          @metrics["overtime"]
        end
      end

      class Window
        attr_accessor :size, :interval
        def initialize(key, name)
          size, interval = Sidekiq::Limiter.redis do |conn|
            conn.hmget("lmtr-w-#{name}", "size".freeze, "interval".freeze)
          end
          @size = size.to_i
          @interval = interval.to_sym
        end

        def rate; "#{@size} / #{@interval}"; end
        def type; :window; end
        def type_name; 'Window'.freeze; end
      end

      class Bucket
        attr_accessor :size, :interval

        def initialize(key, name)
          @name = name
          size, interval = Sidekiq::Limiter.redis do |conn|
            conn.hmget("lmtr-b-#{name}", "size".freeze, "interval".freeze)
          end
          @size = size.to_i
          @interval = interval.to_sym
        end

        def rate; "#{@size} / #{@interval}"; end
        def type; :bucket; end
        def type_name; 'Bucket'.freeze; end

        def history
          # Pull historic usage from Redis
          # bucket only
          now = Time.now

          buckets = []
          data = Sidekiq::Limiter.redis do |conn|
            conn.pipelined do
              data_count.times do |idx|
                time = now - (idx * increment)
                bkt = bucket_name(time)
                buckets << time
                conn.incrby(bkt, 0)
              end
            end
          end

          # Array#to_h wasn't added until Ruby 2.1
          Hash[*buckets.zip(data).flatten]
        end

        private

        def bucket_name(time=Time.now)
          bucket = case @interval
                   when :second; time.strftime("%M:%S".freeze)
                   when :minute; time.strftime("%H:%M".freeze)
                   when :hour; time.strftime("%F:%H".freeze)
                   when :day; time.strftime("%F".freeze)
                   else raise ArgumentError, "Unknown interval: #{@interval}"
                   end

          "lmtr-bdata-#{@name}-#{bucket}"
        end

        def increment
          @incr ||= case @interval
          when :second; 1
          when :minute; 60
          when :hour; 3600
          when :day; 86400
          end
        end

        def data_count
          @count ||= case @interval
          when :second; 60
          when :minute; 60
          when :hour; 24
          when :day; 30
          end
        end
      end

      attr_accessor :key, :name

      def initialize(key)
        @key = key
        m = /lmtr-(.)-(.+)/.match(key)
        raise ArgumentError, "Unknown limiter key #{key}" unless m
        @type = m[1]
        @name = m[2]
      end

      def helper
        @helper ||= begin
          if @type == 'c'.freeze
            Concurrent.new(@key, @name)
          elsif @type == 'w'.freeze
            Window.new(@key, @name)
          elsif @type == 'b'.freeze
            Bucket.new(@key, @name)
          else
            raise ArgumentError, "Unknown limiter key #{key}"
          end
        end
      end

      def method_missing(*args)
        helper.send(*args)
      end

    end
  end
end
