require 'sidekiq/worker'

module Sidekiq
  module Worker
    attr_accessor :bid

    def batch
      @sbatch ||= Sidekiq::Batch.new(bid) if bid
    end

    # Verify the job is still considered part of the batch.
    def valid_within_batch?
      raise RuntimeError, "Not a member of a batch" unless bid

      Sidekiq::Batch.redis(bid) do |conn|
        conn.sismember("b-#{bid}-jids", jid)
      end
    end
  end
end
