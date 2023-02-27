require 'singleton'
require 'uri'

module Sidekiq
  module Enterprise
    # The leader runs this code nightly to scan the current Sidekiq
    # Enterprise cluster and upload census data to contribsys.  This collection
    # is allowed under Section 11 of the COMM-LICENSE.
    #
    # It uploads aggregate metrics and license info, never any source code or user data.
    #
    class Census
      include Singleton unless $TESTING
      include Sidekiq::Util

      def initialize
        @creds = begin
          creds = Bundler.settings["enterprise.contribsys.com"]
          if creds
            u, p = creds.split(":")
            if u && u.to_i(16) > 0 && p
              creds
            else
              nil
            end
          else
            nil
          end
        end
        @tag = Sidekiq.options[:tag]
        @rver = RUBY_VERSION
        @sver = Sidekiq::VERSION
        @ever = Sidekiq::Enterprise::VERSION
      end

      def start
        return unless valid?
        return if defined?(@thread)

        @thread ||= safe_thread('census') do
          loop do
            amt = pause
            sleep amt if amt > 0
            perform if Sidekiq::Senate.leader?
          end
        end
      end

      private unless $TESTING

      def perform
        uri = URI("https://census.contribsys.com/report")
        uri.query = parameterize

        begin
          respcode = network_call(uri)
          if respcode == 200
          elsif respcode == 404
            # ignore remote errors
          elsif respcode >= 500
            # ignore remote errors
          else
            Sidekiq.logger.warn("Problem talking with the Sidekiq census service: #{respcode} #{res.body}")
          end
        rescue => ex
          Sidekiq.logger.error("Error contacting Sidekiq census service: #{ex}")
        end

        true
      end

      # Leaders send their daily census report during the hour of 3AM Pacific
      def pause(hour = 11)
        tomorrow = Time.now + 86400
        fire = Time.utc(tomorrow.year, tomorrow.month, tomorrow.day, hour, minute, tomorrow.sec)
        secs = (fire - Time.now).to_f
        Sidekiq.logger.debug("Next census: #{fire} in #{secs} sec")
        secs
      end

      def valid?
        return false unless @creds
        return false unless env == "production"
        true
      end

      def env
        Sidekiq.options[:environment]
      end

      def minute
        user.to_i(16) % 60
      end

      def user
        @creds.split(":").first
      end

      def scale_metrics
        jobs = 0
        threads = 0
        processes = 0
        procs = Sidekiq::ProcessSet.new
        procs.each do |process|
          # quiet processes don't count
          next if process.stopping?

          processes += 1
          threads += process["concurrency"]
        end
        jobs = Sidekiq::Stats.new.processed
        [jobs, threads, processes]
      end

      def network_call(uri)
        res = Net::HTTP.start(uri.host, uri.port, use_ssl: true) do |http|
          req = Net::HTTP::Get.new(uri)
          req.basic_auth(*@creds.split(":"))
          http.request(req)
        end
        res.code.to_i
      end

      def parameterize
        j, t, p = scale_metrics
        data = {
          v: 1,
          tag: @tag,
          rver: @rver,
          sver: @sver,
          ever: @ever,
          threads: t,
          processes: p,
          jobs: j,
          user: user,
        }
        URI.encode_www_form(data)
      end

    end
  end
end

Sidekiq.configure_server do |config|
  config.on(:leader) do
    census = Sidekiq::Enterprise::Census.instance
    census.start
  end
end
