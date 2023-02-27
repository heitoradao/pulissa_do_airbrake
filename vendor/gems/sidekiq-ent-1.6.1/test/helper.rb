$TESTING = true
ENV['RACK_ENV'] = 'test'
if ENV['COVERAGE']
  require 'simplecov'
  SimpleCov.start do
    add_filter "/test/"
  end
end

require 'minitest/autorun'
require 'minitest/pride'

require 'sidekiq-ent'
require "socket"

Sidekiq.logger.level = Logger::WARN

# https://github.com/jruby/jruby/wiki/ServerSocket
if RUBY_ENGINE != 'jruby'
  ServerSocket = Socket # as shown above
  class Socket
    alias_method :_old_bind, :bind
    def bind(addr, backlog)
      _old_bind(addr)
      listen(backlog)
    end
  end
end

Sidekiq.configure_client do |config|
  config.redis = { size: 50, url: 'redis://localhost:6379/8' }
end
Sidekiq.redis { |c| c.flushdb }

Sidekiq::Enterprise::Scripting.bootstrap

# Create a server which will blow up if more connections
# are made than a given amount.  Used to verify our
# concurrency limiting.


require 'net/http/server'

def random_port
  10000 + rand(20000)
end

def limited_server(port=9987, count=5, &block)
  tokens = Array.new(count) {|idx| idx }

  Net::HTTP::Server.run(:port => port, :background => true, :log => nil) do |request,stream|
    begin
      token = tokens.pop
      if token
        block.call(token)
        [200, {'Content-Type' => 'text/html'}, ['Hello World']]
      else
        [429, {'Content-Type' => 'text/plain'}, ['Exceeded rate limit']]
      end
    rescue => ex
      p [ex.class.name, ex.message]
      p ex.backtrace
    ensure
      tokens.push(token) if token
    end
  end
end

class Sidekiq::Senate
  def lead!
    @leader_until = Time.now.to_f + 100
  end
  def follow!
    @leader_until = 0
  end
end

def stop_logging
  oldlvl = Sidekiq::Logging.logger.level
  begin
    Sidekiq::Logging.logger.level = Logger::FATAL
    yield
  ensure
    Sidekiq::Logging.logger.level = oldlvl
  end
end
