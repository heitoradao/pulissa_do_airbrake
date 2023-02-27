require_relative 'helper'

class TestLimitMiddleware < Minitest::Test
  def setup
    Sidekiq.redis{|c|c.flushdb}
  end

  class SomeWorker
    include Sidekiq::Worker
  end

  def test_normal
    s = Sidekiq::ScheduledSet.new
    assert_equal 0, s.size

    m = Sidekiq::Limiter::Middleware.new
    w = SomeWorker.new
    w.jid = '123456'
    q = 'default'
    job = { }
    m.call(w, job, q) do
    end
    assert_equal 0, s.size
  end

  def test_reschedule
    lvl = Sidekiq.logger.level
    s = Sidekiq::ScheduledSet.new
    assert_equal 0, s.size

    limiter = Sidekiq::Limiter.bucket(:stripe, 10, :second)

    m = Sidekiq::Limiter::Middleware.new
    w = SomeWorker.new
    w.jid = '123456'

    q = 'default'
    job = { 'class' => SomeWorker.name, 'args' => [] }
    21.times do |idx|
      assert_equal idx, s.size
      assert_equal idx, job['overrated'] || 0

      m.call(w, job, q) do
        raise Sidekiq::Limiter::OverLimit, limiter
      end
    end

    Sidekiq.logger.level = Logger::ERROR
    assert_equal 21, s.size
    assert_equal 21, job['overrated'] || 0
    assert_raises Sidekiq::Limiter::OverLimit do
      m.call(w, job, q) do
        raise Sidekiq::Limiter::OverLimit, limiter
      end
    end

  ensure
    Sidekiq.logger.level = lvl
  end

  class UrRateLimitedDude < RuntimeError; end

  def test_handle_custom_exceptions
    s = Sidekiq::ScheduledSet.new
    assert_equal 0, s.size

    Sidekiq::Limiter.errors << UrRateLimitedDude
    m = Sidekiq::Limiter::Middleware.new
    w = SomeWorker.new
    w.jid = '123456'

    q = 'default'
    job = { 'class' => SomeWorker.name, 'args' => [] }

    assert_raises RuntimeError do
      m.call(w, job, q) do
        raise "boom"
      end
    end
    assert_equal 0, s.size
    assert_nil job['overrated']

    m.call(w, job, q) do
      raise UrRateLimitedDude
    end
    assert_equal 1, s.size
    assert_equal 1, job['overrated']
  end

  def test_batches_see_success
    b = Sidekiq::Batch.new
    jid = nil
    b.jobs do
      jid = SomeWorker.perform_async
    end

    bs = Sidekiq::Batch::Status.new(b.bid)
    assert_equal 1, bs.pending
    assert_equal 1, bs.total

    job = { 'bid' => b.bid, 'class' => 'SomeWorker', 'jid' => jid, 'args' => [] }

    sw = SomeWorker.new
    bm = Sidekiq::Batch::Server.new
    lm = Sidekiq::Limiter::Middleware.new

    lm.call(sw, job, 'default') do
      bm.call(sw, job, 'default') do
        raise Sidekiq::Limiter::OverLimit, 'oops'
      end
    end

    bs = Sidekiq::Batch::Status.new(b.bid)
    assert_equal 1, bs.pending
    assert_equal 1, bs.total
  end


  def test_custom_backoff
    x = 3
    Sidekiq::Limiter.backoff = ->(limiter, job) do
      return x
    end

    s = Sidekiq::ScheduledSet.new
    assert_equal 0, s.size

    limiter = Sidekiq::Limiter.bucket(:stripe, 10, :second)
    m = Sidekiq::Limiter::Middleware.new
    w = SomeWorker.new
    w.jid = '123456'

    q = 'default'
    job = { 'class' => SomeWorker.name, 'args' => [] }

    m.call(w, job, q) do
      raise Sidekiq::Limiter::OverLimit, limiter
    end
    assert_equal 1, s.size
    assert_equal 1, job['overrated']
    assert_in_delta Time.now.to_f + 3, s.first.at.to_f, 0.01
    s.clear

    Sidekiq::Limiter.backoff = ->(l, j) do
      return 'b'
    end

    stop_logging do
      m.call(w, job, q) do
        raise Sidekiq::Limiter::OverLimit, limiter
      end
    end
    assert_equal 1, s.size
    assert_equal 2, job['overrated']
    assert_in_delta Time.now.to_f + 300, s.first.at.to_f, 0.01

  ensure
    Sidekiq::Limiter.backoff = Sidekiq::Limiter::DEFAULT_BACKOFF
  end

end
