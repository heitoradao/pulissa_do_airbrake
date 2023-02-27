require 'sidekiq/util'

module Sidekiq::Pro

  Message = Struct.new(:verb, :payload)

  ##
  # Allows for real-time configuration updates to be published to all
  # Sidekiq processes cluster-wise.  For example, pausing and unpausing
  # queues is now instantaneous via this mechanism.
  #
  # Event listeners register their interest via #register and must
  # supply a `notify(verb, payload)` method.
  #
  #   Sidekiq::Pro::Config.register(self)
  #
  # You can broadcast a config event via `publish`:
  #
  #   Sidekiq::Pro::Config.publish(:boom, { 'some' => 'info' })
  #
  # The `notify` method on all registered listeners on all Sidekiq processes
  # will be called.
  #
  # NOTE: pubsub is not persistent so you need to ensure that your listeners
  # can pull the current state of the system from Redis.
  #
  class ConfigListener
    include Sidekiq::Util

    CHANNEL = "sidekiq:config"

    def initialize
      @thread = nil
      @done = false
      @handlers = []
    end

    def register(handler)
      @handlers << handler
    end

    # Takes a connection because it should be called as part of a larger
    # `multi` block to update Redis.
    def publish(conn, verb, payload)
      conn.publish(CHANNEL, Marshal.dump(Message.new(verb, payload)))
    end

    def start
      @thread ||= safe_thread("config", &method(:listen))
    end

    def terminate
      @done = true
      @thread.raise Sidekiq::Shutdown
    end

    private

    def listen
      while !@done
        begin
          Sidekiq.redis do |conn|
            conn.psubscribe(CHANNEL) do |on|
              on.pmessage do |pattern, channel, msg|
                message = Marshal.load(msg)
                @handlers.each do |watcher|
                  watcher.notify(message.verb, message.payload)
                end
              end
            end
          end
        rescue Sidekiq::Shutdown
        rescue => ex
          handle_exception(ex)
          sleep 1
        end
      end
    end

  end

  Config = ConfigListener.new
end

Sidekiq.configure_server do |config|
  config.on(:startup) do
    Sidekiq::Pro::Config.start
  end
  config.on(:quiet) do
    Sidekiq::Pro::Config.terminate
  end
end
