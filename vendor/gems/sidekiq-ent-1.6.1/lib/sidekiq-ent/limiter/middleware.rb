module Sidekiq
  module Limiter
    DEFAULT_BACKOFF = ->(limiter, job) do
      (300 * job['overrated']) + rand(300) + 1
    end

    # An optional set of additional error types which would be
    # treated as a rate limit violation, so the job would automatically
    # be rescheduled as with Sidekiq::Limiter::OverLimit.
    #
    # Sidekiq::Limiter.errors << MyApp::TooMuch
    # Sidekiq::Limiter.errors = [Foo::Error, MyApp::Limited]
    class << self
      attr_accessor :errors
    end
    self.errors = []

    # The backoff proc controls when a job will be rescheduled
    # to try again if an operation would violate a rate limit.
    # This is NOT safe to dynamically change at runtime, it should be set
    # once at initialization time only.
    #
    # Takes the job and, optionally, a limiter.  The limiter is not
    # available if a user-defined error triggered the rate limiter.
    #
    # Sidekiq::Limiter.backoff = ->(limiter, job) do
    #   return 60 # always wait 60 seconds and try again
    # end
    class << self
      attr_accessor :backoff
    end
    self.backoff = DEFAULT_BACKOFF

    class Middleware

      def call(worker, job, queue)
        begin
          yield
        rescue Sidekiq::Limiter::OverLimit => ex
          if job['overrated'] && job['overrated'] > maximum_reschedules(ex.limiter, job)
            worker.logger.warn { "Limiter '#{ex.limiter}' over rate limit for too long, giving up!" }
            raise ex
          else
            worker.logger.info { "Limiter '#{ex.limiter}' over rate limit, rescheduling for later" }
            reschedule(worker, ex, job, ex.limiter)
          end
        rescue *Sidekiq::Limiter.errors => x
          if job['overrated'] && job['overrated'] > maximum_reschedules(nil, job)
            worker.logger.warn { "#{x}: over rate limit for too long, giving up!" }
            raise x
          else
            worker.logger.info { "#{x}: over rate limit, rescheduling for later" }
            reschedule(worker, x, job, nil)
          end
        end
      end

      private

      def reschedule(worker, ex, msg, limiter)
        msg['overrated'] ||= 0
        delay = Sidekiq::Limiter.backoff.call(limiter, msg)
        delayf = delay.to_f
        if delayf == 0.0
          Sidekiq.logger.error("Limiter backoff returned an invalid value: #{delay.inspect}")
          delayf = 300
        end
        msg['at'] = Time.now.to_f + delayf
        msg['overrated'] += 1

        Sidekiq::Client.push(msg)
      end

      def maximum_reschedules(limiter, job)
        20
      end

    end
  end
end

Sidekiq.configure_server do |config|
  config.server_middleware do |chain|
    # When an OverLimit error is raised within a job, we want
    # the batch middleware to see it first and mark the job as a failure.
    # Once that's done, we can get the error, quietly swallow the error
    # and reschedule the job so it is not considered a retry.
    chain.insert_before Sidekiq::Batch::Server, Sidekiq::Limiter::Middleware
  end
end
