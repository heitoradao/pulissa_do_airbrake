require_relative 'helper'
require 'net/http'

class TestLimitConcurrency < Minitest::Test
  def setup
    Sidekiq.redis{|c| c.flushdb}
  end

  def test_naming
    assert_raises ArgumentError do
      Sidekiq::Limiter.concurrent("my limiter", 5, :wait_timeout => 0.5)
    end
  end

  def test_simple
    limiter = Sidekiq::Limiter.concurrent(:simple, 1, :wait_timeout => 2)

    assert_equal(7776000, Sidekiq::Limiter.redis { |r| r.ttl("lmtr-cfree-simple") })
    assert_equal(-2, Sidekiq::Limiter.redis { |r| r.ttl("lmtr-cused-simple") })
    assert_equal(-2, Sidekiq::Limiter.redis { |r| r.ttl("lmtr-cpend-simple") })

    limiter.within_limit do
      # derp
    end

    assert_equal(7776000, Sidekiq::Limiter.redis { |r| r.ttl("lmtr-cfree-simple") })
    assert_equal(-2, Sidekiq::Limiter.redis { |r| r.ttl("lmtr-cused-simple") })
    assert_equal(-2, Sidekiq::Limiter.redis { |r| r.ttl("lmtr-cpend-simple") })
  end

  def test_limiting
    port = random_port
    server = limited_server(port) do
      sleep 0.2
    end

    assert_raises ArgumentError do
      Sidekiq::Limiter.concurrent(:tester, 5, :wait_timeout => 0.5)
    end

    # Only allow 5 concurrent connections to the server so
    # this block should take 1 second to execute, since the
    # last three will need to pause for 1 second.
    clients = []

    limiter = Sidekiq::Limiter.concurrent(:tester, 5, :wait_timeout => 2)
    start = Time.now
    8.times do |idx|
      clients << Thread.new do
        limiter.within_limit do
          resp = Net::HTTP.get_response(URI("http://127.0.0.1:#{port}/"))
          assert_equal "200", resp.code
        end
      end
    end

    clients.each(&:join)
    # first 5 are handled immediately, next three have to wait
    assert_in_delta Time.now - start, 0.4, 0.1
  ensure
    server.stop if server
  end

  def test_limited
    port = random_port
    server = limited_server(port) do
      sleep 0.2
    end

    # Verify that we allow 5 connections through
    # and the rest timeout with an OverLimit error.
    clients = []

    limiter = Sidekiq::Limiter.concurrent(:tester, 5, :wait_timeout => 0)
    start = Time.now
    8.times do |idx|
      clients << Thread.new do
        limiter.within_limit do
          resp = Net::HTTP.get_response(URI("http://127.0.0.1:#{port}/"))
          resp.code
        end
      end
    end

    results = clients.map do |t|
      begin
        t.value
      rescue => ex
        ex.class.to_s
      end
    end
    assert_equal %w(200 200 200 200 200 Sidekiq::Limiter::OverLimit Sidekiq::Limiter::OverLimit Sidekiq::Limiter::OverLimit), results.sort
    # first 5 are handled immediately, next three have to wait
    assert_in_delta Time.now - start, 0.2, 0.1

    stat = limiter.status
    assert_equal 5, stat.size
    assert_equal 5, stat.available
    assert_equal 0, stat.used
  ensure
    server.stop if server
  end

  def test_limited_with_ignore
    port = random_port
    server = limited_server(port) do
      sleep 0.2
    end

    # Verify that we allow 5 connections through
    # and the rest return silently
    clients = []

    limiter = Sidekiq::Limiter.concurrent(:tester, 5, wait_timeout: 0, policy: :skip)
    start = Time.now
    8.times do |idx|
      clients << Thread.new do
        limiter.within_limit do
          resp = Net::HTTP.get_response(URI("http://127.0.0.1:#{port}/"))
          resp.code
        end
      end
    end

    results = clients.map do |t|
      begin
        t.value
      rescue => ex
        ex.class.to_s
      end
    end
    assert_equal %w(200 200 200 200 200 nil nil nil), results.map {|x| x ? x : "nil"}.sort
    # first 5 are handled immediately, next three have to wait
    assert_in_delta Time.now - start, 0.2, 0.1

    stat = limiter.status
    assert_equal 5, stat.size
    assert_equal 5, stat.available
    assert_equal 0, stat.used
  ensure
    server.stop if server
  end

  def test_reclaim_old_locks
    port = random_port
    limited_server(port) do
      sleep 0.5
    end

    clients = []

    limiter = Sidekiq::Limiter.concurrent(:tester, 1, :wait_timeout => 0, :lock_timeout => 0.1)
    stat = limiter.status
    assert_equal 1, stat.size
    assert_equal 1, stat.available
    assert_equal 0, stat.used

    start = Time.now
    2.times do |idx|
      clients << Thread.new do
        limiter.within_limit do
          resp = Net::HTTP.get_response(URI("http://127.0.0.1:#{port}/"))
          resp.code
        end
      end
      sleep 0.1
    end
    results = clients.map do |t|
      begin
        t.value
      rescue => ex
        ex.class.to_s
      end
    end

    assert_equal %w(200 200), results.sort
    # first 5 are handled immediately, next three have to wait
    assert_in_delta Time.now - start, 0.5, 0.2

    stat = limiter.status
    assert_equal 1, stat.size
    assert_equal 1, stat.available
    assert_equal 0, stat.used

    # verify size change works
    limiter = Sidekiq::Limiter.concurrent(:tester, 4, :wait_timeout => 0, :lock_timeout => 0.1)
    stat = limiter.status
    assert_equal 4, stat.size
    assert_equal 4, stat.available
    assert_equal 0, stat.used
  end

  def test_concurrent_threadsafe
    port = random_port
    limited_server(port) do
      sleep 0.5
    end

    clients = []
    user_id = 12

    3.times do |idx|
      clients << Thread.new do
        limiter = Sidekiq::Limiter.concurrent(user_id, 1, :wait_timeout => 0)
        limiter.within_limit do
          resp = Net::HTTP.get_response(URI("http://127.0.0.1:#{port}/"))
          resp.code
        end
      end
    end
    results = clients.map do |t|
      begin
        t.value
      rescue => ex
        ex.class.to_s
      end
    end

    expected = ["200", "Sidekiq::Limiter::OverLimit", "Sidekiq::Limiter::OverLimit"]
    assert_equal expected, results.sort
  end

  def test_nested_usage
    limiter = Sidekiq::Limiter.concurrent(Time.now.to_i.to_s, 1, :wait_timeout => 0)
    assert_raises Sidekiq::Limiter::OverLimit do
      limiter.within_limit do
        raise Sidekiq::Limiter::OverLimit, nil
      end
    end

    count = Sidekiq::Limiter.redis do |conn|
      conn.hget(limiter.key, "overrated")
    end.to_i
    assert_equal 0, count
  end

end
