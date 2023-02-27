require_relative 'helper'

class TestLimitBucket < Minitest::Test
  def setup
    Sidekiq.redis{|c| c.flushdb}
  end

  def test_naming
    assert_raises ArgumentError do
      Sidekiq::Limiter.bucket("my limiter", 5, :second, :wait_timeout => 1)
    end
  end

  def test_up_to_limit
    limiter = Sidekiq::Limiter.bucket(:tes_ter, 5, :second, :wait_timeout => 1)

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

  def test_over_limit_doesnt_pause
    limiter = Sidekiq::Limiter.bucket(:"tes-ter", 5, :minute, :wait_timeout => 1)
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
    limiter = Sidekiq::Limiter.bucket(:"tes-ter", 5, :second, :wait_timeout => 1)
    start = Time.now
    count = 0
    7.times do
      limiter.within_limit do
        count += 1
      end
    end
    assert_equal 7, count
    assert_in_delta Time.now.to_f - start.to_f, 0.1, 1.0
  end

  def test_over_limit_raises
    limiter = Sidekiq::Limiter.bucket(:tester, 5, :second, :wait_timeout => 0)
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
    limiter = Sidekiq::Limiter.bucket(:tester, 5, :second, wait_timeout: 0, policy: :skip)
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
