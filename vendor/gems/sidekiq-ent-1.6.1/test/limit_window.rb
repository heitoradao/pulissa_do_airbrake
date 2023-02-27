require_relative 'helper'

class TestLimitWindow < Minitest::Test
  def setup
    Sidekiq.redis{|c| c.flushdb}
  end

  def test_validation
    assert_raises ArgumentError do
      Sidekiq::Limiter.window("my limiter", 5, :second, :wait_timeout => 1)
    end
    assert_raises ArgumentError do
      Sidekiq::Limiter.window("my_limiter", 5, "60", :wait_timeout => 1)
    end
  end

  def test_up_to_limit
    limiter = Sidekiq::Limiter.window(:tes_ter, 5, :second, :wait_timeout => 1)

    start = Time.now
    count = 0
    5.times do
      limiter.within_limit do
        count += 1
      end
    end
    assert_equal 5, count
    assert_in_delta Time.now.to_f - start.to_f, 0.0, 0.1
  end

  def test_numeric_interval_greater_than_waitfor_doesnt_pause
    limiter = Sidekiq::Limiter.window(:"tes-ter", 5, 2, :wait_timeout => 1)
    start = Time.now
    count = 0
    assert_raises Sidekiq::Limiter::OverLimit do
      12.times do
        limiter.within_limit do
          count += 1
        end
      end
    end
    assert_equal 5, count
    assert_in_delta Time.now.to_f - start.to_f, 0.1, 0.1
  end

  def test_over_limit_doesnt_pause
    limiter = Sidekiq::Limiter.window(:"tes-ter", 5, :minute, :wait_timeout => 1)
    start = Time.now
    count = 0
    assert_raises Sidekiq::Limiter::OverLimit do
      12.times do
        limiter.within_limit do
          count += 1
        end
      end
    end
    assert_equal 5, count
    assert_in_delta Time.now.to_f - start.to_f, 0.1, 0.1
  end

  def test_over_limit_pauses
    limiter = Sidekiq::Limiter.window(:"tes-ter", 5, :second, :wait_timeout => 1)
    start = Time.now
    count = 0
    7.times do
      limiter.within_limit do
        count += 1
      end
    end
    assert_equal 7, count
    assert_in_delta Time.now.to_f - start.to_f, 1.5, 0.5
  end

  def test_over_limit_raises
    limiter = Sidekiq::Limiter.window(:tester, 5, :second, :wait_timeout => 0)
    start = Time.now
    count = 0
    assert_raises Sidekiq::Limiter::OverLimit do
      6.times do
        limiter.within_limit do
          count += 1
        end
      end
    end
    assert_in_delta Time.now.to_f - start.to_f, 0, 0.1
  end

  def test_over_limit_ignores
    limiter = Sidekiq::Limiter.window(:tester, 5, :second, wait_timeout: 0, policy: :skip)
    start = Time.now
    count = 0

    7.times do
      limiter.within_limit do
        count += 1
      end
    end
    assert_in_delta Time.now.to_f - start.to_f, 0, 0.1
    assert_equal 5, count

    stat = limiter.status
    assert_equal 5, stat.size
  end
end
