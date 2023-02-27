module Sidekiq
  class Batch
    ##
    # A snapshot in time of the current Batch status.
    #
    # * total - number of jobs in this batch.
    # * pending - number of jobs which have not reported success yet.
    # * failures - number of jobs which have failed.
    #
    # Batch job(s) can fail and be retried through Sidekiq's retry feature.
    # For this reason, a batch is considered complete once all jobs have
    # been executed, even if one or more executions was a failure.
    class Status
      attr_reader :bid
      attr_reader :failures

      def initialize(bid)
        @bid = bid
        load_data(bid)
      end

      def load_data(bid)
        @props, @failures, @completed = Sidekiq::Batch.redis(bid) do |conn|
          conn.pipelined do
            conn.hgetall("b-#{bid}")
            conn.hlen("b-#{bid}-failinfo")
            conn.scard("b-#{bid}-complete")
          end
        end
        @pending = @props['pending'].to_i
        @total = @props['total'].to_i
        raise NoSuchBatch, "Couldn't find Batch #{bid} in redis" if @props.empty?
      end

      def parent_bid
        @props['parent']
      end

      def parent_batch
        if parent_bid
          @parent_batch ||= Sidekiq::Batch.new(parent_bid)
        end
      end

      def parent
        if parent_bid
          @parent ||= Status.new(parent_bid)
        end
      end

      def child_count
        @props['kids'].to_i
      end

      def pending
        @pending
      end

      def total
        @total
      end

      def expiry
        (@props['expiry'] || Batch::EXPIRY).to_i
      end

      def description
        @props['description']
      end

      def callbacks
        @callbacks ||= Sidekiq.load_json(@props['callbacks'])
      end

      def created_at
        Time.at(@props['created_at'].to_f)
      end

      def expires_at
        created_at + expiry
      end

      # Remove all info about this batch from Redis.  The main batch
      # data hash is kept around for 24 hours so it can be queried for status
      # after success.
      #
      # Returns the bid if anything was deleted, nil if nothing was deleted.
      def delete
        result, _ = Sidekiq::Batch.redis(bid) do |conn|
          conn.pipelined do
            conn.hsetnx("b-#{bid}", "deleted", "1")
            conn.del "b-#{bid}-failinfo",
                     "b-#{bid}-notify",
                     "b-#{bid}-cbsucc",
                     "b-#{bid}-success",
                     "b-#{bid}-complete",
                     "b-#{bid}-jids"
            conn.zrem('batches'.freeze, bid)
            conn.expire "b-#{bid}", Sidekiq::Batch::LINGER
          end
        end
        result ? bid : nil
      end

      def deleted?
        @props['deleted']
      end

      def jids
        Sidekiq::Batch.redis(bid) do |conn|
          conn.smembers("b-#{bid}-jids")
        end
      end

      def include?(jid)
        Sidekiq::Batch.redis(bid) do |conn|
          conn.sismember("b-#{bid}-jids", jid)
        end
      end

      # returns true if any or all jids in the batch have been invalidated.
      def invalidated?
        count = @props["invalid"].to_i
        count != 0
      end

      def success_pct
        return 0 if total == 0
        ((total - pending) / Float(total)) * 100
      end

      def pending_pct
        return 0 if total == 0
        ((pending - failures) / Float(total)) * 100
      end

      def failure_pct
        return 0 if total == 0
        (failures / Float(total)) * 100
      end

      # A Batch is considered complete when no jobs are pending or
      # the only pending jobs have already failed.  Any child batches
      # must have also completed.
      def complete?
        @props['deleted'] == '2' || (pending == failures && (child_count == 0 || child_count == @completed))
      end

      def join
        poll
      end

      def poll(polling_sleep = 1)
        while true
          begin
            # 3640 status is a snapshot in time, we must get a
            # fresh status on each iteration so we don't
            # make a decision based on old/cached data
            st = Sidekiq::Batch::Status.new(bid)
            break if st.deleted? || st.complete?
            sleep polling_sleep
          rescue Sidekiq::Batch::NoSuchBatch
            break
          end
        end
        true
      end

      Failure = Struct.new(:jid, :error_class, :error_message, :backtrace)

      # Batches store job failure info in a Hash, keyed off the bid.
      # The Hash contains { jid => [class name, error message] }
      def failure_info
        failures = Sidekiq::Batch.redis(bid) {|conn| conn.hgetall("b-#{bid}-failinfo") }
        failures.map {|jid, json| Failure.new(jid, *Sidekiq.load_json(json)) }
      end

      def data
        {
          :is_complete => complete?,
          :bid => bid,
          :total => total,
          :pending => pending,
          :description => description,
          :failures => failures,
          :created_at => created_at.to_f,
          :fail_info => failure_info.map do |err|
            { :jid => err.jid, :error_class => err.error_class, :error_message => err.error_message, :backtrace => nil }
          end
        }
      end

      def to_json
        Sidekiq.dump_json data
      end
    end
  end
end
