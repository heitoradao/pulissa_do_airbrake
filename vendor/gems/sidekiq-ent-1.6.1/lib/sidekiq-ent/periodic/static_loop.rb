require 'sidekiq-ent/periodic/cron'
require 'digest/sha1'

module Sidekiq
  module Periodic
    ##
    # A static loop is a periodic job which is registered with the
    # system upon startup, e.g. your typical cron job.
    #
    class StaticLoop
      attr_accessor :schedule, :options

      def initialize(schedule, klass, options)
        @schedule = schedule
        @klass = klass
        @options = options
        @cron = Sidekiq::CronParser.new(@schedule)
      end

      def next_occurrence(now=Time.now)
        @cron.next(now).to_i
      end

      def job_hash
        @options
      end

      def klass
        @klass
      end

      def lid
        @lid ||= Digest::SHA1.hexdigest([@schedule.to_s, @klass.to_s, @options.inspect].join("|"))
      end
    end
  end
end
