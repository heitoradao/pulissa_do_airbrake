require 'sidekiq/fetch'
require 'sidekiq/pro/config'

module Sidekiq::Pro

  # Adds pause queue support to Sidekiq's basic fetch strategy.
  class BasicFetch < ::Sidekiq::BasicFetch
    def initialize(options, events=Sidekiq::Pro::Config)
      super(options)

      members = Sidekiq.redis do |conn|
        conn.smembers("paused")
      end
      @paused = Set.new(Array(members))
      @changed = true
      @original = options[:queues].dup

      events.register(self)
    end

    def notify(verb, payload)
      if verb == :pause
        @paused << payload
        @changed = true
      elsif verb == :unpause
        @paused.delete payload
        @changed = true
      end
    end

    def queues_cmd
      if @changed
        queues = (@original - @paused.to_a).map {|q| "queue:#{q}" }
        if @strictly_ordered_queues
          queues = queues.uniq
          queues << TIMEOUT
        end
        @queues = queues
        @changed = nil
      end
      super
    end

  end
end
