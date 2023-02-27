require_relative 'helper'

class TestScripting < Minitest::Test
  def test_handles_flushed_scripts
    Sidekiq::Enterprise::Scripting.bootstrap

    mock = Minitest::Mock.new
    mock.expect(:with, nil) do
      raise Redis::CommandError, "NOSCRIPT foo"
    end
    mock.expect(:with, nil)
    mock.expect(:with, 27)
    assert_equal 27, Sidekiq::Enterprise::Scripting.call(:foo, [], [], mock)
    mock.verify
  end

  def test_handles_old_redis
    mock = Minitest::Mock.new
    mock.expect(:with, nil) do
      raise Redis::CommandError, "got unknown command EVAL"
    end
    assert_raises RuntimeError do
      Sidekiq::Enterprise::Scripting.call(:foo, [], [], mock)
    end
    mock.verify
  end
end
