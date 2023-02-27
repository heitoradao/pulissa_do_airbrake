module Sidekiq

  ##
  # The ShardSet is the set of Redis servers which Sidekiq can
  # use to store data and perform operations.
  #
  # In the ShardSet, the index of the shard is **critical** and
  # should never be changed.  For that reason, you pass the
  # complete set of shards at once, rather than adding them
  # one at a time.  You can safely add a new shard to the end
  # of the list but you CANNOT remove or move shards in the list.
  #
  #   Sidekiq::Shards.set [POOL1, POOL2, POOL3, POOL4]
  #
  # Since the shard set is Enumerable, you can do nice things like:
  #
  #   total_enqueued_default_jobs = Sidekiq::Shards.map { Sidekiq::Queue.new.size }.inject(:+)
  #
  class ShardSet
    include Enumerable

    def shards
      @shards ||= [Sidekiq.redis_pool]
    end

    def each(&block)
      prev = Thread.current[:sidekiq_redis_pool]
      shards.each do |x|
        Thread.current[:sidekiq_redis_pool] = x
        block.call(x)
      end
    ensure
      Thread.current[:sidekiq_redis_pool] = prev
    end

    def [](idx)
      shards[idx]
    end

    def set(*args)
      @shards = args.flatten
    end

    def enabled?
      shards.size > 1
    end

    ##
    # Return a random shard index
    def random_index
      return 0 if shards.size == 1

      rand(shards.size)
    end

    def on(idx, &block)
      prev = Thread.current[:sidekiq_redis_pool]
      Thread.current[:sidekiq_redis_pool] = shards[idx]
      block.call
    ensure
      Thread.current[:sidekiq_redis_pool] = prev
    end

  end

  Shards = ShardSet.new
end

Sidekiq.configure_server do |config|
  config.on(:startup) do
    # touch attribute while on main thread to avoid any
    # initialization race conditions.
    Sidekiq::Shards.shards
  end
end
