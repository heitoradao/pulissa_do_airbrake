module Sidekiq
  class Batch
    class Callback
      include Sidekiq::Worker

      SUCCESS = 'success'
      COMPLETE = 'complete'

      def perform(event, bid, queue='default')
        logger.debug { "BID-#{bid} #{event}" }
        status = Status.new(bid)
        send(event.to_sym, status, queue) if status
      end

      private

      def constantize(str)
        names = str.split('::')
        names.shift if names.empty? || names.first.empty?

        constant = Object
        names.each do |name|
          constant = constant.const_defined?(name) ? constant.const_get(name) : constant.const_missing(name)
        end
        constant
      end

      def execute_callback(status, event, target, options)
        klass_name, method = target.split('#')
        klass = constantize(klass_name)
        meth = method || "on_#{event}"
        inst = klass.new
        inst.jid = jid if inst.respond_to?(:jid)
        inst.send(meth, status, options)
        Sidekiq::Pro.metrics.increment("batches.#{event}")
      end

      def success(status, queue)
        # Run any success callbacks
        if status.callbacks[SUCCESS]
          status.callbacks[SUCCESS].each do |hash|
            hash.each_pair do |target, options|
              execute_callback(status, SUCCESS, target, options)
            end
          end
        end

        bid = status.bid
        pb = status.parent_bid
        if pb
          # Check to see if our success means the parent is now successful
          key = "b-#{pb}"
          success = "b-#{pb}-success"
          cbcomp, cbsucc, _, _, pending, successes, kids, q = Sidekiq::Batch.redis(pb) do |conn|
            conn.multi do
              conn.get("#{key}-notify")
              conn.get("#{key}-cbsucc")
              conn.sadd(success, status.bid)
              conn.expire(success, Sidekiq::Batch::EXPIRY)
              conn.hincrby(key, "pending".freeze, 0)
              conn.scard(success)
              conn.hincrby(key, "kids".freeze, 0)
              conn.hget(key, "cbq".freeze)
            end
          end

          if pending == 0 && successes == kids && cbcomp && cbcomp.to_i > 1 && !cbsucc && needs_success?(pb)
            enqueue_callback(queue, ['success'.freeze, pb, q || queue])
          end
        end

        Sidekiq::Batch.redis(bid) do |conn|
          conn.multi do
            conn.hsetnx("b-#{bid}", "deleted", "2")
            conn.del "b-#{bid}-failinfo", "b-#{bid}-success", "b-#{bid}-complete", "b-#{bid}-jids"
            conn.zrem('batches'.freeze, bid)
            conn.expire "b-#{bid}", Sidekiq::Batch::LINGER
            # we can't delete these two or running callbacks inline will recurse forever
            conn.expire "b-#{bid}-notify", 60
            conn.expire "b-#{bid}-cbsucc", 60
            conn.publish("batch-#{bid}", '$')
          end
        end

      end

      def complete(status, queue)
        # Run the complete callbacks for this batch
        if status.callbacks[COMPLETE]
          status.callbacks[COMPLETE].each do |hash|
            hash.each_pair do |target, options|
              execute_callback(status, COMPLETE, target, options)
            end
          end
        end

        # if we have a parent batch, check to see if our
        # completion means that it is complete now and we need to
        # fire the complete callback for it.
        pb = status.parent_bid
        if pb
          cbcomp, _, _, complete, children, pending, fails, q  = Sidekiq::Batch.redis(pb) do |conn|
            conn.multi do
              conn.get("b-#{pb}-notify")

              key = "b-#{pb}-complete"
              conn.sadd(key, status.bid)
              conn.expire(key, Sidekiq::Batch::EXPIRY)
              conn.scard(key)

              key = "b-#{pb}"
              conn.hincrby(key, "kids".freeze, 0)
              conn.hincrby(key, "pending".freeze, 0)
              conn.hlen("#{key}-failinfo")
              conn.hget(key, "cbq".freeze)
            end
          end

          if complete == children && pending == fails && !cbcomp && needs_complete?(pb)
            enqueue_callback(queue, ['complete'.freeze, pb, q || queue])
          end
        end

        # Mark ourselves as complete now so that our success callback can
        # be fired.
        bid = status.bid
        _, _, pending, children, bsucc, q = Sidekiq::Batch.redis(bid) do |conn|
          conn.multi do
            # 1 means the complete callback has been created
            # 2 means the complete callback has run successfully
            conn.incrby("b-#{bid}-notify", 1)
            conn.publish("batch-#{bid}", '!')
            if bid.length == 16
              conn.get("b-#{bid}-pending")
            else
              key = "b-#{bid}"
              conn.hincrby(key, "pending".freeze, 0)
              conn.hincrby(key, "kids".freeze, 0)
              conn.scard("#{key}-success")
              conn.hget(key, "cbq".freeze)
            end
          end
        end

        if pending.to_i == 0 && children == bsucc && needs_success?(bid)
          enqueue_callback(queue, ['success'.freeze, bid, q || queue])
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

      def enqueue_callback(queue, args)
        Sidekiq::Client.push('class' => Sidekiq::Batch::Callback,
                             'queue' => queue,
                             'args' => args)
      end
    end

    # Backwards compat for any Lifecycle jobs which are sitting in
    # Redis during the 2.0 upgrade.
    Lifecycle = Callback
  end
end
