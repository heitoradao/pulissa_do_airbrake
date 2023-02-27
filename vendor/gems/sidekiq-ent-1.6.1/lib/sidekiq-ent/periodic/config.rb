require 'sidekiq-ent/periodic/static_loop'

module Sidekiq
  module Periodic
    class Config

      # Periodic data lives for 90 days by default
      STATIC_TTL = 90 * 24 * 60 * 60

      def self.instance
        @config ||= Config.new
      end

      # The version is SHA hash of all statically defined jobs.  If
      # any job data changes, the version will change also.  In
      # this way we know when we need to resync job data to Redis
      # by checking a single key.
      attr_reader :version
      attr_reader :work

      def initialize
        @work = {}
        @version = nil
        @locked = false
      end

      def register(schedule, klass, options={})
        raise "Periodic jobs may not be configured after startup" if @locked

        # stringify
        options.keys.each do |key|
          options[key.to_s] = options.delete(key)
        end

        newloop = Sidekiq::Periodic::StaticLoop.new(schedule, klass, options)
        raise ArgumentError, "Already registered periodic job #{klass} with schedule #{schedule}" if @work.has_key?(newloop.lid)
        @work[newloop.lid] = newloop
        newloop.lid
      end

      ###
      # Internal only below
      ###

      def empty?
        @work.empty?
      end

      def persist
        Sidekiq.redis do |conn|
          conn.pipelined do
            # global key with our current data version
            conn.set('periodic-version', version, ex: STATIC_TTL)
            conn.del("loops-#{version}")
            work.values.each do |lop|
              # create a set of all LIDs in this version
              conn.sadd("loops-#{version}", lop.lid)
              # create a hash for each periodic job with fields
              conn.hmset("loop-#{lop.lid}", 'schedule'.freeze, lop.schedule,
                                            'class'.freeze, lop.klass,
                                            *lop.options.to_a)
              conn.expire("loop-#{lop.lid}", STATIC_TTL)
            end
            conn.expire("loops-#{version}", STATIC_TTL)
          end
        end
      end

      def clear
        Sidekiq.redis do |conn|
          conn.set('periodic-version', version, ex: STATIC_TTL)
        end
      end

      def finish!
        @locked = true
        sha = Digest::SHA1.new
        @work.keys.sort.each {|key| sha.update(key) }
        @version = sha.hexdigest

        require 'sidekiq-ent/heap'
        q = Containers::MinHeap.new

        @work.values.each do |cycle|
          q.push(cycle.next_occurrence, cycle)
        end

        q
      end
    end
  end
end
