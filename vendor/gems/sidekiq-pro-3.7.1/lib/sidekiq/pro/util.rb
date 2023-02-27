module Sidekiq
  module Pro
    class << self
      attr_accessor :logger
    end
    self.logger = Sidekiq.logger

    def self.deprecated(msg)
      logger.warn "#{msg} is deprecated and will be removed in a future release."
      logger.warn "Please switch to Batch callbacks: https://github.com/mperham/sidekiq/wiki/Batches#callbacks"
      logger.warn caller[1]
    end
  end
end
