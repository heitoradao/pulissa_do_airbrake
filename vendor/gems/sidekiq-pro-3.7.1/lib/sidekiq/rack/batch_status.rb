=begin
Using websockets is efficient but requires thin or another event-driven server so
it's possible you'll need to deploy a new server.  Notably it will not work with Unicorn or
Passenger.  Instead we'll add a really simple Rack middleware which allows reasonably efficient
polling.  This will work reasonably well with single-threaded app servers and very well with
multi-threaded app servers like Puma.

Use the middleware via `config.ru`:

    require 'sidekiq/rack/batch_status'
    use Sidekiq::Rack::BatchStatus
    run Rails::Application

Then you can query the server to get a JSON blob of data about a batch
by passing the BID.

   http://server.example.org/batch_status/abcdef1234567890.json

=end

module Sidekiq
  module Rack

    class BatchStatus

      def initialize(app, options={})
        @app = app
        @mount = /\A#{Regexp.escape(options[:mount] || '/batch_status')}\/([0-9a-zA-Z_\-]{14,16}).json\z/
      end

      def call(env)
        return @app.call(env) if env['PATH_INFO'] !~ @mount

        begin
          batch = Sidekiq::Batch::Status.new($1)
          [200, {'Content-Type' => 'application/json'}, [batch.to_json]]
        rescue => ex
          return [401, {'Content-Type' => 'text/plain'}, [ex.message]]
        end
      end

    end

  end
end
