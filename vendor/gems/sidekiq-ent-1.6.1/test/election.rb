require_relative 'helper'

require 'sidekiq-ent/senate'

describe Sidekiq::Senate do
  before do
    Sidekiq.redis {|c| c.flushdb}
  end

  describe 'leader' do

    it 'can be elected' do
      l = Sidekiq::Senate.new
      assert_equal true, l.stage_coup!
      assert_equal true, l.leader?
      assert_equal true, l.stop!
      assert_equal false, l.leader?

      begin
        Sidekiq.logger.level = Logger::ERROR
        # can't update if we aren't leader
        assert_equal false, l.send(:update_leader)
        assert_equal true, l.stage_coup!
        assert_equal true, l.send(:update_leader)
      ensure
        Sidekiq.logger.level = Logger::WARN
      end
    end

    it 'runs reliable elections' do
      l = Sidekiq::Senate.new
      assert_equal false, l.leader?
      assert_equal 60, l.send(:interval)
      l.election
      assert_equal true, l.leader?
      l.election
      assert_equal true, l.leader?
      assert_equal 15, l.send(:interval)
    end
  end
end
