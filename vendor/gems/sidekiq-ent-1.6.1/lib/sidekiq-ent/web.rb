require 'sidekiq/pro/web'
require 'sidekiq-ent/limiter'

module Sidekiq::Enterprise
  module Web
    ROOT = File.expand_path('../../../web', __FILE__)

    module Helpers
      def product_version
        "Sidekiq #{Sidekiq::VERSION} / Pro #{Sidekiq::Pro::VERSION} / Ent #{Sidekiq::Enterprise::VERSION}"
      end
    end

    def self.registered(app)
      app.helpers ::Sidekiq::Enterprise::Web::Helpers

      # periodic loops
      app.get "/loops/:lid" do
        @loop = Sidekiq::Periodic::Loop.new(params[:lid])
        render(:erb, File.read("#{ROOT}/views/loop.erb"))
      end
      app.get "/loops" do
        @loops = Sidekiq::Periodic::LoopSet.new
        render(:erb, File.read("#{ROOT}/views/loops.erb"))
      end
      app.tabs['Cron'] = 'loops'.freeze

      # rate limiting
      app.get "/limits/:name" do
        @limit = Sidekiq::Limiter::Status.new(params[:name])
        render(:erb, File.read("#{ROOT}/views/#{@limit.type}.erb"))
      end
      app.get "/limits" do
        @limits = Sidekiq::LimiterSet.new
        (@next, @results) = @limits.paginate(params[:page].to_i)
        render(:erb, File.read("#{ROOT}/views/limits.erb"))
      end
      app.tabs['Limits'] = 'limits'.freeze

      app.settings.locales << File.expand_path('locales', ROOT)
    end

    class Authorization
      def initialize(app, &block)
        @app = app
        @authorize = block
      end

      def call(env)
        path = env['PATH_INFO']
        method = env['REQUEST_METHOD'].upcase
        if @authorize.call(env, method, path)
          @app.call(env)
        else
          Sidekiq.logger.warn("Unauthorized Sidekiq::Web request #{method} #{path}")
          return [403, {"Content-Type" => "text/plain"}, ["Unauthorized action"]]
        end
      end
    end

  end
end

class Sidekiq::Web
  def self.authorize(&block)
    Sidekiq::Web.use Sidekiq::Enterprise::Web::Authorization, &block
  end
end

::Sidekiq::Web.register Sidekiq::Enterprise::Web
