require 'sidekiq/util'

module Sidekiq

  ##
  # The Senate abstraction allows a cluster of Sidekiq processes,
  # all talking through a shared Redis instance, to elect and maintain
  # a leader.
  #
  # Leaders renew leadership every 15 seconds.  If they have not renewed within
  # 60 seconds, any other connected processes can assume leadership.
  #
  # When shutting down, the Leader "steps down" so that a new process can immediately
  # assume leadership upon startup.  Note that a "quiet" leader process still runs
  # its leadership tasks.  It does not step down until TERM - this is to ensure cron
  # jobs are still fired even during a long quiet period.
  #
  class Senate
    include Sidekiq::Util

    TTL = 60

    Sidekiq::Enterprise::Scripting::LUA[:leader_unlock] = <<-SCRIPT
      if redis.call("get",KEYS[1]) == ARGV[1]
      then
          return redis.call("del",KEYS[1])
      else
          return 0
      end
    SCRIPT

    Sidekiq::Enterprise::Scripting::LUA[:leader_update] = <<-SCRIPT
      if redis.call("get",KEYS[1]) == ARGV[1]
      then
          return redis.call("expire",KEYS[1], #{TTL})
      else
          return 0
      end
    SCRIPT

    def initialize
      @key = "dear-leader".freeze
      @leader_until = 0
      @keys = [@key]
      @args = [identity]
      @done = false
      @thread = nil
      @listener = nil
      @sleeper = ConnectionPool::TimedStack.new
    end

    def self.instance
      @senate ||= Sidekiq::Senate.new
    end

    def self.leader?
      instance.leader?
    end

    def terminate
      @done = true
      @sleeper << 0
    end

    def start(listener=nil)
      @listener = listener
      @thread ||= safe_thread("senate", &method(:cycle))
    end

    def stop!
      return unless leader?

      Sidekiq.logger.info { "Leader stepping down" }
      result = Sidekiq::Enterprise::Scripting.call(:leader_unlock, @keys, @args)
      if result == 0
        logger.info { "Ignoring my leader stepdown request as I wasn't the leader." }
      end
      @leader_until = 0
      result != 0
    end

    def leader?
      @leader_until > Time.now.to_f
    end

    def stage_coup!
      result = Sidekiq.redis do |conn|
        conn.set(@key, identity, ex: TTL, nx: true)
      end
      if result
        @leader_until = Time.now.to_f + TTL
        logger.info { "Gained leadership of the cluster" }
        @listener.fire_event(:leader) if @listener
      end
      result
    end

    def cycle
      stage_coup!
      # stagger all followers in time so the swarm doesn't all pound
      # Redis at the same millisecond.
      sleep(rand(interval)) unless leader?

      while !@done
        begin
          election
        rescue => ex
          handle_exception(ex)
        end

        begin
          @sleeper.pop(interval)
        rescue Timeout::Error
        end
      end

      stop!
    end

    def election
      if leader?
        if update_leader
          @leader_until = Time.now.to_f + TTL
          #logger.debug { "Leader until #{Time.at(@leader_until)}" }
        else
          @leader_until = 0
          logger.info { "Lost leadership of Sidekiq cluster" }
        end
      else
        # all processes will blindly try to setnx
        # the leadership key.  If it works, they've
        # become leader.
        stage_coup!
      end
    end

    private

    def update_leader
      result = Sidekiq::Enterprise::Scripting.call(:leader_update, @keys, @args)
      result != 0
    end

    def interval
      leader? ? TTL/4 : TTL
    end

  end
end
