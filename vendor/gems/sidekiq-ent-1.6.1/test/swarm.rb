require_relative 'helper'
require 'sidekiq-ent/swarm'

class TestSwarm < Minitest::Test
  def swarm(*args)
    Sidekiq::Enterprise::Swarm.new(*args).tap do |x|
      x.io = StringIO.new
    end
  end

  def test_initialize
    s = swarm({}, [], Minitest::Mock.new)
    assert_equal Concurrent.processor_count, s.count
    assert_equal [], s.children

    s = swarm({ 'SIDEKIQ_COUNT' => 18 }, [], Minitest::Mock.new)
    assert_equal 18, s.count
    assert_equal [], s.children
  end

  def test_monitor
    signals = Minitest::Mock.new
    signals.expect(:trap, nil, [String])
    signals.expect(:trap, nil, [String])
    signals.expect(:trap, nil, [String])
    signals.expect(:trap, nil, [String])

    s = swarm({}, [], signals)
    s.monitor(false)

    signals.verify
  end

  def test_memory_tracking
    s = swarm({'SIDEKIQ_MAXMEM_MB' => 10}, [])
    assert_match(/^ps /, s.ps_cmd)

    s.children = [456,654]
    s.stub(:ps_output, "123 456 17890\n123 654 19239") do
      assert_equal [456,654], s.check_children(Set.new)
    end
    s.stub(:ps_output, "123 456 17890\n123 654 19239") do
      assert_equal [654], s.check_children(Set.new([456]))
    end
    s.stub(:ps_output, "123 456 7890\n123 654 9239") do
      assert_equal [], s.check_children(Set.new)
    end
  end

  def test_spawn
    s = swarm({'SIDEKIQ_COUNT' => '2'}, [])
    assert_equal [], s.children
    s.stub(:forkit, 88) do
      s.start
    end
    assert_equal [88, 88], s.children
  end
end
