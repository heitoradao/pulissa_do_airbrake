require_relative 'helper'
require 'sidekiq-ent/web'
require 'sidekiq-ent/periodic/config'
require 'sidekiq-ent/periodic/manager'
require 'sidekiq-ent/senate'
require 'rack/test'

class TestWeb < Minitest::Test
  include Rack::Test::Methods

  def setup
    Sidekiq.redis { |c| c.flushdb }
    Sidekiq::Web.instance_variable_set(:@app, nil)
    Sidekiq::Web.instance_variable_set(:@middlewares, nil)
  end

  def app
    Sidekiq::Web
  end

  def test_home
    get '/'
    assert_equal 200, last_response.status
    assert_match(/status-idle/, last_response.body)
    assert_match(/Limits/, last_response.body)
  end

  def test_authorization
    Sidekiq::Web.authorize do |env, method, path|
      method == 'GET'
    end

    get '/limits'
    assert_equal 200, last_response.status
    assert_match(/No limits found/, last_response.body)
    post '/filter/retries'
    assert_equal 403, last_response.status
  end

  def test_limits_empty_index
    Sidekiq::Limiter.redis {|c| c.flushdb }

    get '/limits'
    assert_equal 200, last_response.status
    assert_match(/No limits found/, last_response.body)
  end

  def test_limits_full_index
    Sidekiq::Limiter.concurrent(:acmecorp, 39)
    Sidekiq::Limiter.bucket(:stripe, 5, :second)

    get '/limits'
    assert_equal 200, last_response.status
    assert_match(/acmecorp/, last_response.body)
    assert_match(/stripe/, last_response.body)
  end

  def test_display_concurrent_limiter
    c = Sidekiq::Limiter.concurrent(:acmecorp, 39)

    get "/limits/#{c.key}"
    assert_equal 200, last_response.status
    assert_match(/acmecorp/, last_response.body)
    assert_match(/oncurrent/, last_response.body)
    assert_match(/39/, last_response.body)
    refute_match(/td>0\.01/, last_response.body)

    c.within_limit do
      sleep 0.01
    end

    get "/limits/#{c.key}"
    assert_equal 200, last_response.status
    assert_match(/acmecorp/, last_response.body)
    assert_match(/oncurrent/, last_response.body)
    assert_match(/td>0\.01/, last_response.body) # verify held_time increments
  end

  def test_display_window_limiter
    q = Sidekiq::Limiter.window(:stripe, 5, :second)

    get "/limits/#{q.key}"
    assert_equal 200, last_response.status
    assert_match(/stripe/, last_response.body)
    assert_match(/Window/, last_response.body)

    q.within_limit do
      sleep 0.01
    end

    get "/limits/#{q.key}"
    assert_equal 200, last_response.status
    assert_match(/stripe/, last_response.body)
    assert_match(/Window/, last_response.body)
  end

  def test_display_bucket_limiter
    q = Sidekiq::Limiter.bucket(:stripe, 5, :second)

    get "/limits/#{q.key}"
    assert_equal 200, last_response.status
    assert_match(/stripe/, last_response.body)
    assert_match(/Bucket/, last_response.body)

    q.within_limit do
      sleep 0.01
    end

    get "/limits/#{q.key}"
    assert_equal 200, last_response.status
    assert_match(/stripe/, last_response.body)
    assert_match(/Bucket/, last_response.body)
  end

  def test_loops_index
    # verify empty loops page
    get '/loops'
    assert_equal(200, last_response.status)
    assert_match(/No periodic jobs found/, last_response.body)

    cfg = Sidekiq::Periodic::Config.new
    cfg.register("* * * * *", "HardWorker", foo: 'xyzzy')
    cfg.register("*/5 * 4 * *", "HardWorker", bar: 'nacho')
    cfg.finish!
    cfg.persist

    # verify show basic listing of loops
    get '/loops'
    assert_equal(200, last_response.status)
    assert_match(/xyzzy/, last_response.body)
    assert_match(/HardWorker/, last_response.body)

    cfg = Sidekiq::Periodic::Config.new
    cfg.register("* * * * *", "HardWorker", foo: 'abccde')
    cfg.register("*/5 * 4 * *", "HardWorker", bar: 'hjkl')
    cfg.finish!
    cfg.persist

    # verify version changes and old loops aren't shown
    get '/loops'
    assert_equal(200, last_response.status)
    refute_match(/xyzzy/, last_response.body)
    assert_match(/abccde/, last_response.body)
    assert_match(/HardWorker/, last_response.body)
  end

  def test_display_loop
    cfg = Sidekiq::Periodic::Config.new
    cfg.register("*/5 * * * *", "HardWorker", bar: 'hjkl')
    cfg.finish!
    cfg.persist

    Sidekiq::Senate.instance.lead!
    mgr = Sidekiq::Periodic::Manager.new
    mgr.persist(cfg)

    now = Time.now
    60.times do |x|
      mgr.process(now+(x*60)+60)
    end

    set = Sidekiq::Periodic::LoopSet.new
    assert_equal 1, set.size
    lop = set.first
    assert_equal 12, lop.history.size

    get "/loops/#{lop.lid}"
    assert_match(/#{lop.lid}/, last_response.body)
    assert_match(/#{lop.history[0][0]}/, last_response.body)
  end

end
