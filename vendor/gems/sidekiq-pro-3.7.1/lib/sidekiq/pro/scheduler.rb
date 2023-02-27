require 'sidekiq/scheduled'
require 'sidekiq/pro/scripting'

# Implements Lua-based schedule enqueuer
module Sidekiq
  module Scheduled
    class FastEnq < Sidekiq::Scheduled::Enq
      def initialize
        prefix = Sidekiq.redis { |conn| conn.respond_to?(:namespace) ? conn.namespace : nil }
        @prefix = prefix.to_s == '' ? '' : "#{prefix}:"
      end

      def enqueue_jobs(now=Time.now.to_f.to_s, sorted_sets=Sidekiq::Scheduled::SETS)
        sorted_sets.map do |sset|
          total = 0
          loop do
            count = Sidekiq::Pro::Scripting.call(:fast_enqueue, [sset, "queues"], [now, @prefix])
            break total + count if count != 100
            total += count
          end
        end
      end
    end
  end
end
