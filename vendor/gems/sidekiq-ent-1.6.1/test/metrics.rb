require_relative 'helper'

require 'sidekiq-ent/metrics'
require 'statsd'

class TestMetrics < Minitest::Test
  def setup
    Sidekiq::Senate.instance.lead!
    Sidekiq.redis(&:flushdb)
  end

  class MyWorker
    include Sidekiq::Worker
  end

  class FakeStatsd
    attr_accessor :gauges
    def initialize
      @gauges = {}
    end
    def batch
      yield self
    end
    def gauge(name, value)
      @gauges[name] = value
    end
    def gauge_value(name)
      @gauges[name]
    end
  end

  def test_collect
    s = Sidekiq::Enterprise::History.new
    assert_equal 30, s.interval
    assert_raises RuntimeError do
      s.start
    end
    s.statsd = Statsd.new('127.0.0.1', 8125)
    s.start
    s.capture
    s.stop
  end

  def test_save_history
    # just verify it doesn't blow up
    Sidekiq.save_history(nil)
  end

  def test_fake_collect
    Sidekiq::Client.enqueue_to(:mike, MyWorker)
    Sidekiq::Client.enqueue_to(:mike, MyWorker)
    Sidekiq::Client.enqueue_to(:mike, MyWorker)
    MyWorker.perform_async
    MyWorker.perform_in(3)

    aq = Sidekiq::Queue.all
    s = Sidekiq::Enterprise::History.new
    s.statsd = f = FakeStatsd.new
    s.capture
    assert_equal 7 + aq.size, f.gauges.size
    assert_equal 4, f.gauge_value("sidekiq.enqueued")
    assert_equal 1, f.gauge_value("sidekiq.scheduled")
    assert_equal 1, f.gauge_value("sidekiq.enqueued.default")
    assert_equal 3, f.gauge_value("sidekiq.enqueued.mike")
  end

  def test_calls_custom_block
    s = Sidekiq::Enterprise::History.new
    s.statsd = f = FakeStatsd.new
    ran = false
    s.custom = lambda do |statsd, stats|
      assert_equal f, statsd
      assert_equal Sidekiq::Stats, stats.class
      ran = true
    end
    s.capture
    assert ran
  end
end
