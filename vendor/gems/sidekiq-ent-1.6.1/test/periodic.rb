require_relative 'helper'

require 'sidekiq-ent/periodic'
require 'sidekiq-ent/periodic/config'
require 'sidekiq-ent/periodic/manager'

class TestPeriodic < Minitest::Test
  def setup
    Sidekiq::Senate.instance.lead!
  end

  class MyWorker
  end

  def test_configuration
    Sidekiq.periodic
    Sidekiq.periodic {|mgr| refute_nil mgr }

    cfg = Sidekiq::Periodic::Config.new
    assert cfg.register('* * * * *', MyWorker)
    assert_raises ArgumentError do
      cfg.register('* * * * *', MyWorker)
    end
    assert_raises ArgumentError do
      cfg.register('*/15 * * * * *', MyWorker)
    end

    refute cfg.empty?

    cfg = Sidekiq::Periodic::Config.new
    cfg.clear
    assert_equal true, cfg.empty?

    loops = Sidekiq::Periodic::LoopSet.new
    assert_equal 0, loops.size
  end

  def test_versioning
    cfg = Sidekiq::Periodic::Config.new
    assert cfg.register('*/5 * * * *', MyWorker)
    assert cfg.register('2 * * * *', MyWorker)
    assert_nil cfg.version
    cfg.finish!
    refute_nil cfg.version
    assert_raises RuntimeError do
      cfg.register('2 * * * *', MyWorker)
    end
  end

  def test_processing
    cfg = Sidekiq::Periodic::Config.new
    mylid = cfg.register('*/5 * * * *', 'MyWorker')
    assert mylid
    alid = cfg.register('*/3 * * * *', 'AWorker')
    assert alid

    mgr = Sidekiq::Periodic::Manager.new
    mgr.persist(cfg)

    time = Time.now.to_f
    60.times do |idx|
      secs = mgr.send(:seconds_until_next_minute, time + idx)
      assert secs < 60
      assert secs > 0
    end

    now = Time.now
    count = 0
    60.times do |x|
      (time, *result) = mgr.process(now+(x*60)+60)
      if time.min % 5 == 0
        assert_includes(result, mylid)
        count += 1
      end
      if time.min % 3 == 0
        assert_includes(result, alid)
        count += 1
      end
    end
    assert_equal 32, count

    set = Sidekiq::Periodic::LoopSet.new
    assert_equal 2, set.size
    set.each do |loup|
      assert_includes [12, 20], loup.history.size
      assert loup.next_run
    end
  end
end
