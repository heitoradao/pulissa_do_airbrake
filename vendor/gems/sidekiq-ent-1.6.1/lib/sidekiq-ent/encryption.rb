require 'base64'

module Sidekiq
  # keep these names short to minimize overhead
  Enc = Struct.new(:c, :tag, :iv, :v, :blob)

  module Enterprise

    # This class can auto-{en,de}crypt a secret argument for
    # sensitive payloads.  You enable the feature by
    # initializing it:
    #
    #   Sidekiq::Enterprise::Crypto.enable(active_version: 1) do |version|
    #     <return key>
    #   end
    #
    # where the block must return the bytes of the symmetric key to use
    # for encryption and decryption of the given version.  With key rotation,
    # you bump the active_version and supply the new key for that new version.
    #
    # You can create a random key using OpenSSL and `irb`:
    #
    #   require 'openssl'
    #   File.open("/tmp/my1.key", "w") { |file| file.write(OpenSSL::Cipher.new("aes-256-cbc").random_key) }
    #
    # and now tell Sidekiq about it:
    #
    #   Sidekiq::Enterprise::Crypto.enable(active_version: 1) do |version|
    #     File.read("/tmp/my#{version}.key")
    #   end
    #
    # If using Heroku, you can load the secret key into an ENV var and access that instead.
    #
    # Once you've set up the crypto subsystem, you can activate encryption for a
    # given Worker's arguments like so:
    #
    #     class MySecretWorker
    #       include Sidekiq::Worker
    #       sidekiq_options encrypt: true
    #
    # NOTES
    # ----------
    #
    # * Encryption adds about 100 bytes to the size of arguments.  My MBP can perform about 70,000 enc/dec
    #   round trips per second.
    # * **ONLY** the last argument is encrypted.  Any error message and backtrace will still be plaintext within a job.
    # * The unique jobs feature will not work on encrypted jobs, since all encrypted arguments are unique.
    #
    module Crypto

      def self.enable(opts={}, &block)
        require 'openssl'
        Sidekiq.configure_client do |config|
          config.client_middleware do |chain|
            chain.add Client
          end
        end
        Sidekiq.configure_server do |config|
          config.server_middleware do |chain|
            chain.add Server
          end
          config.client_middleware do |chain|
            chain.add Client
          end
        end
        SCHEMES[true] = Default.new(opts, &block)
      end

      class Default
        KEYS = {}

        def initialize(opts={}, &block)
          @version = opts[:active_version] || 1
          @block = block
          begin
            # GCM requires OpenSSL v1.0.1 or greater.
            # Eager create to verify OpenSSL is ok with this cipher
            OpenSSL::Cipher.new("aes-256-gcm")
            @cipher = "aes-256-gcm"
          rescue => e
            OpenSSL::Cipher.new("aes-256-cbc")
            @cipher = "aes-256-cbc"
            # We would need this fallback for something like Ruby 2.0 on Ubuntu 12.04 LTS
            # Either way, Enterprise can seamlessly decrypt payloads made with either cipher.
            Sidekiq.logger.warn { "Can't activate OpenSSL's GCM mode encryption, which requires OpenSSL >= 1.0.1" }
            Sidekiq.logger.warn { "Falling back to aes-256-cbc on OpenSSL #{OpenSSL::OPENSSL_VERSION}" }
            Sidekiq.logger.warn { "Due to error: #{e.message}" }
            Sidekiq.logger.warn { "CBC mode is not as secure as GCM" }
          end

          KEYS[@version] = block.call(@version)
          Sidekiq.logger.debug { "Enabled secret bag encryption with v#{@version}" }
        end

        def key_for(version)
          KEYS.fetch(version, &@block)
        end

        # Encrypts `thing`, returns a blob of binary data.
        def encrypt(thing)
          enc = OpenSSL::Cipher.new(@cipher)
          enc.encrypt
          enc.key = key_for(@version)

          pay = Enc.new
          pay.c = @cipher
          pay.v = @version
          pay.iv = enc.random_iv
          enc.auth_data = "" if enc.authenticated?
          pay.blob = enc.update(Marshal.dump(thing)) + enc.final
          pay.tag = enc.auth_tag if enc.authenticated?
          Marshal.dump(pay)
        end

        # Decrypts the blob of data, returning `thing`
        def decrypt(blob)
          pay = Marshal.load(blob)

          dec = OpenSSL::Cipher.new(pay.c || "aes-128-cbc")
          dec.decrypt
          dec.key = key_for(pay.v)
          dec.iv = pay.iv
          if dec.authenticated?
            # https://github.com/ruby/openssl/issues/63
            raise ArgumentError, "Invalid encrypted payload" if pay.tag.bytes.size != 16
            dec.auth_tag = pay.tag
            dec.auth_data = ""
          end
          Marshal.load(dec.update(pay.blob) + dec.final)
        end
      end

      SCHEMES = {}
      NON_BASE64 = /[^-A-Za-z0-9_=]/

      class Client
        def call(worker_klass, job, queue, redis_pool)
          raise "Encrypted jobs cannot be declared unique" if job["encrypt"] && job["unique_for"]

          if job['encrypt']
            args = job["args"]
            raise ArgumentError, <<-EOM if args.size < 2
Encrypted workers must have >= 2 arguments and for debugging purposes only the last argument is encrypted:

  def perform(cleartext_for_debugging, also_clear, encrypted)

It is recommended that the last argument be a Hash of sensitive data.
EOM

            last = args[-1]

            # Don't double encrypt the last argument if a middleware
            # wants to re-enqueue a job (e.g. retry or rate limit)
            unless last.is_a?(String) && last !~ NON_BASE64
              args[-1] = encrypt(last, job['encrypt'])
              job["args"] = args
            end
          end
          yield
        end

        def encrypt(thing, scheme)
          Base64.urlsafe_encode64(SCHEMES[scheme].encrypt(thing))
        end
      end

      class Server
        def call(worker, job, queue)
          if job['encrypt']
            old_last = job["args"][-1]
            job["args"][-1] = decrypt(old_last, job['encrypt'])
            begin
              yield
            ensure
              job["args"][-1] = old_last
            end
          else
            yield
          end
        end

        def decrypt(blob, scheme)
          SCHEMES[scheme].decrypt(Base64.urlsafe_decode64(blob))
        end
      end

    end
  end
end
