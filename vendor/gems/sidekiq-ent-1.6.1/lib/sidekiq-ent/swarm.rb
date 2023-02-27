require 'concurrent'
require 'set'

module Sidekiq
  module Enterprise

    # Create a set of child Sidekiq processes underneath a parent
    # process.  We use Bundler.require to pull in the entire set of
    # gems used by the application so we get benefit of CoW memory sharing.
    #
    # Note that we fork the children before loading the app so we won't get
    # as much memory saving as we could otherwise. App preload is fraught
    # with peril; gem preload is safer.
    class Swarm
      attr_accessor :count, :children, :io

      def initialize(env=ENV, argv=ARGV, signal=Signal)
        @env = env
        @argv = argv
        @signal = signal
        @io = STDOUT

        obsolete = %w(COUNT INDEX MAXMEM_KB)
        obsolete.each {|x| log("#{x} is deprecated and will be removed in 2.0, use SIDEKIQ_#{x} instead") if env.has_key?(x) }

        @count = Integer(env['SIDEKIQ_COUNT'] || env['COUNT'] || Concurrent.processor_count)
        @index = Integer(env['SIDEKIQ_INDEX'] || env['INDEX'] || 0)
        @children = []
        @trackmem = File.exist?("/proc")
        @stopping = false
        @max_kb = (env['SIDEKIQ_MAXMEM_KB'] || env['MAXMEM_KB']).to_i
        @max_kb = env['SIDEKIQ_MAXMEM_MB'].to_i * 1024 if @max_kb == 0
        @last_spawn = Time.now.to_f
      end

      def log(str)
        @io.puts("[swarm] #{str}")
      end

      def start_and_monitor
        start
        monitor
      end

      def monitor(wait=true)
        @signal.trap('TERM') { @stopping = true; signal('TERM') }
        @signal.trap('INT') { @stopping = true; signal('TERM') }
        @signal.trap('USR1') { @stopping = true; signal('USR1') }
        @signal.trap('TSTP') { @stopping = true; signal('TSTP') }

        track_memory if @max_kb > 0 && ps_cmd
        start = Time.now
        created = 0

        while wait
          begin
            (pid, status) = ::Process.waitpid2(-1)
            if !@stopping
              # If a child Sidekiq creates a grandchild process and doesn't wait() for the grandchild PID
              # correctly, our wait() can return here.  If it's not one of our child PIDs, ignore it
              # and continue waiting. See #3138
              next unless children.index(pid)

              now = Time.now
              just_started = now - start < 10
              if just_started && created >= count
                # If we spawn COUNT more children within 10 seconds of startup,
                # assume the worst and prematutely exit.
                log "Children dying rapidly upon fork, assuming app is unable to start"
                @stopping = true
                signal("TERM")
                exit(-1)
              else !just_started && @last_spawn + 1 > now.to_f
                # Don't allow us to spawn more than one child per second.
                # A faster rate means an error condition that likely needs manual intervention.
                sleep 1
              end

              log "Child exited, PID #{pid}, code #{status.exitstatus}, restarting..."
              @last_spawn = Time.now.to_f
              idx = children.index(pid)
              spawn(idx)
              created += 1
            end
          rescue Errno::ECHILD
            raise "Unexpected state: no children and not stopping!" unless @stopping
            return
          end
        end
      end

      def track_memory
        @memthread ||= Thread.new do
          log "Starting memory monitoring with max RSS of #{@max_kb}KB"
          last = Set.new
          until @stopping do
            check_children(last)
            sleep 35
          end
        end
      end

      def check_children(last)
        now = []
        output = ps_output
        output.each_line do |line|
          (_, pid, rss) = line.split.map(&:to_i)
          if last.include?(pid)
            # do nothing, waiting for it to terminate
          elsif rss > @max_kb && children.index(pid)
            log "Process #{pid} too large at #{rss}KB, stopping it..."
            terminate_sidekiq(pid)
            now << pid
          end
        end
        last.clear
        last.merge(now)
        now
      end

      def ps_output
        `#{ps_cmd}`
      end

      def terminate_sidekiq(pid)
        killer = ::Process.spawn("kill -TSTP #{pid} && sleep 60 && kill -TERM #{pid}")
        ::Process.detach(killer)
      end

      def ps_cmd
        @cmd ||= begin
          case RUBY_PLATFORM
          when /darwin/
            "ps -o ppid,pid,rss | grep ^#{$$}"
          when /linux/
            "ps ho ppid,pid,rss --ppid #{$$}"
          else
            raise "Unknown platform: #{RUBY_PLATFORM}, memory monitoring not available, please open an issue"
          end
        end
      end

      def signal(sig)
        children.dup.each {|pid| kill(sig, pid) }
      end

      def kill(sig, pid)
        begin
          ::Process.kill(sig, pid)
        rescue Errno::ESRCH
        rescue => ex
          log "Unexpected signal error: #{[pid, sig, ex].inspect}"
        end
      end

      def start
        # Work around this bug by promoting all current objects to OLD.
        # https://bugs.ruby-lang.org/issues/11164
        3.times { GC.start }

        @argv << '-i'
        @argv << '0'
        count.times do |idx|
          spawn(idx)
        end
        $0 = "sidekiqswarm, managing #{count} processes"
      end

      def spawn(idx)
        @argv[@argv.size-1] = (@index + idx).to_s
        children[idx] = forkit
      end

      def forkit
        fork do
          begin
            cli = Sidekiq::CLI.instance
            cli.parse
            cli.run
          rescue => e
            raise e if $DEBUG
            log e.message
            log e.backtrace.join("\n")
            exit 1
          end
        end
      end

    end
  end
end
