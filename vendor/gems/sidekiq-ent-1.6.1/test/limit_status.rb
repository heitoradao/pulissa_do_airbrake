require_relative 'helper'

class TestLimitStatus < Minitest::Test
  def setup
    Sidekiq.redis{|c| c.flushdb}
  end

  def test_bucket_history
    limiter = Sidekiq::Limiter.bucket(:tester, 5, :second, :wait_timeout => 1)
    limiter.within_limit do
      # nothing
    end

    status = Sidekiq::Limiter::Status.new(limiter.key)
    assert_equal :bucket, status.type
    assert_equal 'Bucket', status.type_name
    b = status.history
    assert_equal 60, b.size
    assert_equal 1, b.values[0]
    assert_equal 0, b.values[1]
  end
end


