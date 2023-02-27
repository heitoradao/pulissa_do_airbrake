require 'sidekiq/batch/callback'

module Sidekiq
  class Batch
    class Client

      def call(worker_class, msg, queue, redis_pool)
        batch = Thread.current[:sidekiq_batch]
        if batch
          msg['bid'.freeze] = batch.bid
          result = yield
          batch.register(msg['jid'.freeze]) if result
          result
        else
          yield
        end
      end

    end

    class Server
      def call(worker, msg, queue)
        worker.bid = bid = msg['bid'.freeze]
        if bid
          successful = false
          begin
            yield

            # job is now finished successfully, we don't want Sidekiq::Shutdown
            # to make it run again.
            Thread.handle_interrupt(Sidekiq::Shutdown => :never) do
              add_success(bid, msg['jid'.freeze], queue)
              successful = true
            end
            # noop so Ruby can raise Sidekiq::Shutdown here
            successful = true
          rescue Sidekiq::Shutdown
            # Two cases to handle here:
            # 1. Batch job runs long and Sidekiq raises Sidekiq::Shutdown during job.
            #    We rescue here and reraise without marking the job as a failure.  Otherwise
            #    shutdown can trigger a premature :complete callback.
            # 2. Sidekiq::Shutdown is raised *during* add_{success,failure}.
            #    Once the middleware starts an add_* operation, we want it to finish
            #    atomically so we use handle_interrupt to disable Sidekiq::Shutdown.
            #    We swallow the Shutdown exception so the job is acknowledged in Sidekiq::Processor.
            raise unless successful
          rescue Exception => e
            Thread.handle_interrupt(Sidekiq::Shutdown => :never) do
              add_failure(bid, msg, queue, e)
            end
            raise e
          end
        else
          yield
        end
      end

      private

      def add_success(bid, jid, queue)
        _, cbsucc, cbcomp, pending, _, failures, children, bcomp, bsucc, q = Sidekiq::Batch.redis(bid) do |conn|
          key = "b-#{bid}"
          conn.multi do
            conn.publish("batch-#{bid}", '+'.freeze)
            conn.get("#{key}-cbsucc")
            conn.get("#{key}-notify")
            conn.hincrby(key, "pending".freeze, -1)
            conn.hdel("#{key}-failinfo", jid)
            conn.hlen("#{key}-failinfo")
            conn.hincrby(key, "kids", 0)
            conn.scard("#{key}-complete")
            conn.scard("#{key}-success")
            conn.hget(key, "cbq".freeze)
            conn.srem("#{key}-jids", jid)
          end
        end

        # A batch is complete iff:
        # 1. Its pending job count == failed job count
        # 2. All child batches are complete.
        if pending.to_i == failures.to_i && children == bcomp && !cbcomp && needs_complete?(bid)
          enqueue_callback(q || queue, ['complete'.freeze, bid, q || queue])
        end

        # A batch is successful iff:
        # 1. Its pending job count == 0
        # 2. Its complete callbacks have run.
        # 3. All child batches are successful.
        if pending.to_i == 0 && children == bsucc && cbcomp && cbcomp.to_i > 1 && !cbsucc && needs_success?(bid)
          enqueue_callback(q || queue, ['success'.freeze, bid, q || queue])
        end
      end

      def needs_success?(bid)
        lock, _ = Sidekiq::Batch.redis(bid) do |conn|
          notify_key = "b-#{bid}-cbsucc"
          conn.pipelined do
            conn.setnx(notify_key, 1)
            conn.expire(notify_key, Sidekiq::Batch::EXPIRY)
          end
        end
        lock
      end

      def needs_complete?(bid)
        lock, _ = Sidekiq::Batch.redis(bid) do |conn|
          notify_key = "b-#{bid}-notify"
          conn.pipelined do
            conn.setnx(notify_key, 1)
            conn.expire(notify_key, Sidekiq::Batch::EXPIRY)
          end
        end
        lock
      end

      def add_failure(bid, msg, queue, ex)
        jid = msg['jid'.freeze]

        # App code can stuff all sorts of crazy binary data into the error message
        # that won't convert to JSON.
        m = ex.message.to_s[0, 10_000]
        if m.respond_to?(:scrub!)
          m.force_encoding("utf-8")
          m.scrub!
        end

        info = Sidekiq.dump_json([ex.class.name, m])

        cbcomp, _, _, pending, failures, children, bcomp, q = Sidekiq::Batch.redis(bid) do |conn|
          conn.multi do
            key = "b-#{bid}"
            conn.get("#{key}-notify")
            conn.hset("#{key}-failinfo", jid, info)
            conn.expire("#{key}-failinfo", Batch::EXPIRY)
            conn.hincrby(key, "pending".freeze, 0)
            conn.hlen("#{key}-failinfo")
            conn.hincrby(key, "kids".freeze, 0)
            conn.scard("#{key}-complete")
            conn.hget(key, "cbq".freeze)
            conn.publish("batch-#{bid}", '-'.freeze)
          end
        end
        if pending.to_i == failures && children == bcomp && !cbcomp && needs_complete?(bid)
          enqueue_callback(q || queue, ['complete'.freeze, bid, q || queue])
        end
      end

      def enqueue_callback(queue, args)
        Sidekiq::Client.push('class'.freeze => Sidekiq::Batch::Callback,
                             'queue'.freeze => queue,
                             'args'.freeze => args)
      end
    end
  end
end

Sidekiq.configure_client do |config|
  config.client_middleware do |chain|
    chain.add Sidekiq::Batch::Client
  end
end
Sidekiq.configure_server do |config|
  config.client_middleware do |chain|
    chain.add Sidekiq::Batch::Client
  end
  config.server_middleware do |chain|
    chain.add Sidekiq::Batch::Server
  end
end
