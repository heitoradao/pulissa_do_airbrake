require 'sidekiq/api'
require 'sidekiq/pro/scripting'
require 'sidekiq/pro/config'

module Sidekiq
  def self.redis_version
    @redis_version ||= Sidekiq.redis {|c| c.info["redis_version"] }
  end

  # Allows enumeration of all Batches in Redis.
  # Example:
  #
  #   Sidekiq::BatchSet.new.each do |status|
  #     puts status.bid
  #   end
  class BatchSet
    include Enumerable

    def size
      @_size ||= Sidekiq.redis do |conn|
        conn.zcard("batches".freeze)
      end
    end

    def each
      initial_size = size
      offset_size = 0
      page = -1
      page_size = 50

      loop do
        range_start = page * page_size + offset_size
        range_end   = page * page_size + offset_size + (page_size - 1)
        elements = Sidekiq.redis do |conn|
          conn.zrange "batches".freeze, range_start, range_end, with_scores: true
        end
        break if elements.empty?
        page -= 1
        elements.each do |element, score|
          begin
            yield Sidekiq::Batch::Status.new(element)
          rescue Sidekiq::Batch::NoSuchBatch
          end
        end
        offset_size = initial_size - size
      end
    end

  end

  class Queue
    # Delete a job from the given queue.
    #
    # If the queue is being modified concurrently (e.g. another process is
    # pulling jobs from this queue), it is possible for the job to be "missed".
    # We iterate through the queue backwards to minimize this possibility.
    def delete_job(jid)
      raise ArgumentError, "No JID provided" unless jid
      cursor = -1
      loop do
        # returns one of:
        #   nil - not found
        #   "{ job contents }" - found this job and deleted it
        #   "integer" - cursor location for next search iteration thru queue
        cursor = Sidekiq::Pro::Scripting.call(:queue_delete_by_jid, ["queue:#{name}"], [jid, cursor])
        break if !cursor || cursor[0] == "{"
      end
      cursor
    end

    # Remove all jobs in the queue with the given class.
    # Accepts a String or Class but make sure to pass the fully
    # qualified Class name if you use a String.
    def delete_by_class(klass)
      raise ArgumentError, "No class name provided" unless klass
      size = self.size
      page_size = 50
      result = 0
      r = size - 1
      q = "queue:#{name}"
      klss = klass.to_s

      Sidekiq.redis do |conn|
        while r >= 0 do
          l = r - page_size
          if l < 0 then
            l = 0
          end
          jobs = conn.lrange(q, l, r)
          jobs.each do |jobstr|
            if jobstr.index(klss) then
              job = Sidekiq.load_json(jobstr)
              if job['class'] == klss then
                conn.lrem(q, -1, jobstr)
                result = result + 1
              end
            end
          end
          r = r - page_size
        end
      end
      return result
    end

    def unpause!
      result, _ = Sidekiq.redis do |conn|
        conn.multi do
          conn.srem('paused', name)
          Sidekiq::Pro::Config.publish(conn, :unpause, name)
        end
      end
      result
    end

    def pause!
      result, _ = Sidekiq.redis do |conn|
        conn.multi do
          conn.sadd('paused', name)
          Sidekiq::Pro::Config.publish(conn, :pause, name)
        end
      end
      result
    end

    def paused?
      Sidekiq.redis { |conn| conn.sismember('paused', name) }
    end
  end

  class JobSet
    def find_job(jid)
      Sidekiq.redis do |conn|
        conn.zscan_each(name, :match => "*#{jid}*", :count => 100) do |entry, score|
          job = JSON.parse(entry)
          matched = job["jid"] == jid
          return SortedEntry.new(self, score, entry) if matched
        end
      end
      nil
    end

    # Efficiently scan through a job set, returning any
    # jobs which contain the given substring.
    def scan(match, count = 100, &block)
      regexp = "*#{match}*"
      Sidekiq.redis do |conn|
        if block_given?
          conn.zscan_each(name, :match => regexp, :count => count) do |entry, score|
            yield SortedEntry.new(self, score, entry)
          end
        else
          conn.zscan_each(name, :match => regexp, :count => count).map do |entry, score|
            SortedEntry.new(self, score, entry)
          end
        end
      end
    end

  end
end
