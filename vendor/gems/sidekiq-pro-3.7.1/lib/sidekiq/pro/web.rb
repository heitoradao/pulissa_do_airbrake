require 'sidekiq/web'
require 'sidekiq/batch'

module Sidekiq::Pro
  module Web
    ROOT = File.expand_path('../../../../web', __FILE__)

    module Helpers
      def filtering(which)
        render(:erb, File.read("#{ROOT}/views/filtering.erb"), :locals => { :which => which })
      end

      def product_version
        "Sidekiq v#{Sidekiq::VERSION} / Sidekiq Pro v#{Sidekiq::Pro::VERSION}"
      end

      def search(jobset, substr)
        resultset = jobset.scan(substr)
        @current_page = 1
        @count = @total_size = resultset.size
        resultset
      end

      def filter_link(jid)
        "<a href='#{root_path}filter/retries?substr=#{jid}'>#{jid}</a>"
      end
    end

    # Sinatra only supports class-level configuration so if we want different
    # app instances with different config, we need to create subclasses.
    def self.with(options)
      Class.new(Sidekiq::Web) do
        options.each_pair do |k, v|
          self.settings.send("#{k}=", v)
        end
      end
    end

    def self.registered(app)
      app.helpers ::Sidekiq::Pro::Web::Helpers
      app.set :redis_pool, nil

      app.before do |env=nil, a=nil|
        if a
          # rack
          Thread.current[:sidekiq_redis_pool] = a.settings.redis_pool
        else
          # sinatra
          Thread.current[:sidekiq_redis_pool] = self.settings.redis_pool
        end
      end

      app.after do
        Thread.current[:sidekiq_redis_pool] = nil
      end

      ####
      # Batches
      app.get "/batches/:bid" do
        begin
          @batch = Sidekiq::Batch::Status.new(params[:bid])
          render(:erb, File.read("#{ROOT}/views/batch.erb"))
        rescue Sidekiq::Batch::NoSuchBatch
          redirect "#{root_path}batches"
        end
      end

      app.get "/batches" do
        @count = (params[:count] || 25).to_i
        Sidekiq.redis {|conn| conn.zremrangebyscore('batches'.freeze, '-inf', Time.now.to_f) }
        (@current_page, @total_size, @batches) = page("batches".freeze, params[:page], @count, :reverse => true)
        render(:erb, File.read("#{ROOT}/views/batches.erb"))
      end
      app.tabs['Batches'] = 'batches'.freeze

      ########
      # Filtering
      app.get '/filter/retries' do
        x = params[:substr]
        return redirect "#{root_path}retries" unless x && x == ''

        @retries = search(Sidekiq::RetrySet.new, params[:substr])
        erb :retries
      end

      app.post '/filter/retries' do
        x = params[:substr]
        return redirect "#{root_path}retries" unless x && x != ''

        @retries = search(Sidekiq::RetrySet.new, params[:substr])
        erb :retries
      end

      app.get '/filter/scheduled' do
        x = params[:substr]
        return redirect "#{root_path}scheduled" unless x && x != ''

        @scheduled = search(Sidekiq::ScheduledSet.new, params[:substr])
        erb :scheduled
      end

      app.post '/filter/scheduled' do
        x = params[:substr]
        return redirect "#{root_path}scheduled" unless x && x != ''

        @scheduled = search(Sidekiq::ScheduledSet.new, params[:substr])
        erb :scheduled
      end

      app.get '/filter/dead' do
        x = params[:substr]
        return redirect "#{root_path}morgue" unless x && x != ''

        @dead = search(Sidekiq::DeadSet.new, params[:substr])
        erb :morgue
      end

      app.post '/filter/dead' do
        x = params[:substr]
        return redirect "#{root_path}morgue" unless x && x != ''

        @dead = search(Sidekiq::DeadSet.new, params[:substr])
        erb :morgue
      end

      app.post '/queues/:name/pause' do
        name = params[:name]
        result = Sidekiq::Queue.new(name).pause!
        json({name => result})
      end

      app.post '/queues/:name/unpause' do
        name = params[:name]
        result = Sidekiq::Queue.new(name).unpause!
        json({name => result})
      end

      app.settings.locales << File.expand_path('locales', ROOT)
    end
  end
end

::Sidekiq::Web.register Sidekiq::Pro::Web
