require 'sidekiq/client'

module Sidekiq
  class Client

    #
    # The Sidekiq Batch client adds atomicity to batch definition:
    # all jobs created within the +define+ block are pushed into a
    # temporary array and then all flushed at once to Redis in a single
    # transaction.  This solves two problems:
    #
    # 1. We don't "half-create" a batch due to a networking issue
    # 2. We don't have a "completed" race condition when creating the jobs slower
    #    than we can process them.
    #

    def flush(conn)
      return if collected_payloads.nil?

      collected_payloads.each do |payloads|
        atomic_push(conn, payloads)
      end
    end

    private

    def collected_payloads
      Thread.current[:sidekiq_batch_payloads]
    end

    def raw_push_with_batch(payloads)
      if defining_batch?
        collected_payloads << payloads
        true
      else
        raw_push_without_batch(payloads)
      end
    end

    # FIXME: I tried using Module#prepend but couldn't get it to work.
    # Nothing but stack overflows for me.
    alias_method :raw_push_without_batch, :raw_push
    alias_method :raw_push, :raw_push_with_batch

    def defining_batch?
      Thread.current[:sidekiq_batch_payloads]
    end
  end
end
