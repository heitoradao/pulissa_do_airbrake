require_relative 'helper'

class TestUnique < Minitest::Test
  def setup
    Sidekiq.redis{|c|c.flushdb}
    Sidekiq.client_middleware.add Sidekiq::Enterprise::Unique::Client
  end

  def teardown
    Sidekiq.client_middleware.remove Sidekiq::Enterprise::Unique::Client
  end

  class SomeWorker
    include Sidekiq::Worker
    sidekiq_options unique_for: 10
  end

  class ZeroWorker
    include Sidekiq::Worker
    sidekiq_options unique_for: 0
  end

  class FalseWorker
    include Sidekiq::Worker
    sidekiq_options unique_for: false
  end

  def test_initialize_unique
    assert_includes Sidekiq.client_middleware.entries.map(&:klass), Sidekiq::Enterprise::Unique::Client
    Sidekiq.client_middleware.remove(Sidekiq::Enterprise::Unique::Client)
    refute_includes Sidekiq.client_middleware.entries.map(&:klass), Sidekiq::Enterprise::Unique::Client

    Sidekiq::Enterprise.unique!
    assert_includes Sidekiq.client_middleware.entries.map(&:klass), Sidekiq::Enterprise::Unique::Client
  end

  def test_with_worker
    q = Sidekiq::Queue.new
    assert_equal 0, q.size

    jid = FalseWorker.perform_async(1, 2, 3)
    assert_equal 1, q.size
    assert jid
    jid = FalseWorker.perform_async(1, 2, 3)
    assert_equal 2, q.size
    assert jid
    q.clear

    locked = Sidekiq::Enterprise::Unique.locked?(SomeWorker, [1,2,3])
    refute locked

    jid = SomeWorker.perform_async(1, 2, 3)
    assert_equal 1, q.size
    assert jid

    locked = Sidekiq::Enterprise::Unique.locked?(SomeWorker, [1,2,3])
    assert locked
    locked = Sidekiq::Enterprise::Unique.locked?('foo', SomeWorker, [1,2,3])
    refute locked
    locked = Sidekiq::Enterprise::Unique.locked?(SomeWorker, [1,2,4])
    refute locked

    jid = SomeWorker.perform_async(1, 2, 3)
    assert_equal 1, q.size
    refute jid
    jid = SomeWorker.perform_async(1, 2, 4)
    assert_equal 2, q.size
    assert jid

    ss = Sidekiq::ScheduledSet.new
    jid = SomeWorker.perform_in(10, 1, 2, 4)
    assert_equal 0, ss.size
    refute jid

    jid = SomeWorker.perform_in(10, 1, 2, 5)
    assert_equal 1, ss.size
    assert jid
    jid = SomeWorker.perform_async(1, 2, 5)
    assert_equal 2, q.size
    refute jid
  end

  def test_with_client
    q = Sidekiq::Queue.new
    assert_equal 0, q.size

    kls = SomeWorker.name

    c = Sidekiq::Client.new
    jid = c.push('class' => kls, 'args' => [1,2,3])
    assert_equal 1, q.size
    assert jid

    jid = c.push('class' => kls, 'args' => [1,2,3], 'unique_for' => 10)
    assert_equal 2, q.size
    assert jid

    jid = c.push('class' => kls, 'args' => [1,2,3], 'unique_for' => 10)
    assert_equal 2, q.size
    refute jid

    jid = c.push('class' => SomeWorker, 'args' => [1,2,3], 'unique_for' => 10)
    assert_equal 2, q.size
    refute jid

    jid = c.push('class' => SomeWorker, 'args' => [1,2,3], 'unique_for' => false)
    assert_equal 3, q.size
    assert jid

    jid = c.push('class' => kls, 'args' => [1,2,4], 'unique_for' => 10)
    assert_equal 4, q.size
    assert jid
  end

  def test_scheduled_jobs
    refute_nil SomeWorker.perform_in(10)
    assert_nil SomeWorker.perform_in(10)
    assert_nil SomeWorker.perform_in(10)

    assert_equal 0, Sidekiq::Queue.new.size
    assert_equal 1, Sidekiq::ScheduledSet.new.size

    require 'sidekiq/scheduled'
    scheduler = Sidekiq::Scheduled::Enq.new
    scheduler.enqueue_jobs((Time.now + 10).to_f)

    assert_equal 1, Sidekiq::Queue.new.size
    assert_equal 0, Sidekiq::ScheduledSet.new.size
  end

  def test_scheduled_with_no_uniqueness
    refute_nil ZeroWorker.perform_in(10)
    assert_nil ZeroWorker.perform_in(10)
    assert_nil ZeroWorker.perform_in(10)

    assert_equal 0, Sidekiq::Queue.new.size
    assert_equal 1, Sidekiq::ScheduledSet.new.size

    require 'sidekiq/scheduled'
    scheduler = Sidekiq::Scheduled::Enq.new
    scheduler.enqueue_jobs((Time.now + 10).to_f)

    assert_equal 1, Sidekiq::Queue.new.size
    assert_equal 0, Sidekiq::ScheduledSet.new.size
  end

  def test_server_keeps_lock_on_error_removes_on_success
    jid = SomeWorker.perform_async(1, 2, 3)

    job = Sidekiq::Queue.new.first.item
    m = Sidekiq::Enterprise::Unique::Server.new
    assert_raises RuntimeError do
      m.call(SomeWorker.new, job, 'default') do
        raise 'boom'
      end
    end
    # job failed so lock should still be there
    jid = SomeWorker.perform_async(1, 2, 3)
    refute jid

    m.call(SomeWorker.new, job, 'default') do
      # succeed
    end

    # job succeed so lock should be gone
    jid = SomeWorker.perform_async(1, 2, 3)
    assert jid
  end

  def test_server_unlocks_at_start
    jid = SomeWorker.set(unique_until: :start).perform_async(1, 2, 3)

    job = Sidekiq::Queue.new.first.item
    m = Sidekiq::Enterprise::Unique::Server.new
    assert_raises RuntimeError do
      m.call(SomeWorker.new, job, 'default') do
        raise 'boom'
      end
    end
    # job failed but lock should have been removed
    jid = SomeWorker.perform_async(1, 2, 3)
    assert jid
  end
end
