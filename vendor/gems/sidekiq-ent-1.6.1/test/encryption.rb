require_relative 'helper'

require 'sidekiq-ent/encryption'

describe Sidekiq::Enterprise::Crypto do
  # This basic round trip can run 70,000/sec on my MBP
  it "can encrypt and decrypt data" do
    algo = Sidekiq::Enterprise::Crypto::Default.new do
      OpenSSL::Cipher.new("aes-256-cbc").random_key
    end
    args = [1,2,"345"]

    et = algo.encrypt(args)
    val = algo.decrypt(et)
    assert_equal args, val
  end

  it 'enforces the encrypted argument pattern' do
    mid = Sidekiq::Enterprise::Crypto::Client.new

    job = { 'class' => "FooWorker", "args" => [] }
    mid.call(nil, job, nil, nil) do
      # no problem, not encrypted
    end

    job = { 'class' => "FooWorker", "args" => [], "encrypt" => true }
    assert_raises ArgumentError do
      mid.call(nil, job, nil, nil) do
        fail
      end
    end

    job = { 'class' => "FooWorker", "args" => ["nope"], "encrypt" => true }
    assert_raises ArgumentError do
      mid.call(nil, job, nil, nil) do
        fail
      end
    end
  end


  it "does not double encrypt the last argument" do
    orig = [1, 2, "12398askadsfknc_-123o8=="]
    args = orig.dup
    job = { 'class' => "FooWorker", "args" => args }

    mid = Sidekiq::Enterprise::Crypto::Client.new
    mid.call(nil, job, nil, nil) do
      # no encryption
      assert_equal job["args"], orig
    end

    Sidekiq::Enterprise::Crypto::SCHEMES[true] = Sidekiq::Enterprise::Crypto::Default.new do
      OpenSSL::Cipher.new("aes-256-cbc").random_key
    end
    job["encrypt"] = true
    assert_equal job["args"], orig
    mid.call(nil, job, nil, nil) do
      assert_equal job["args"], orig
    end
    assert_equal job["args"], orig
  end

  it "has client middleware which replaces the job arguments" do
    orig = [1, 2, "foo" => "bar"]
    args = orig.dup
    job = { 'class' => "FooWorker", "args" => args }

    mid = Sidekiq::Enterprise::Crypto::Client.new
    mid.call(nil, job, nil, nil) do
      # no encryption
      assert_equal job["args"], orig
    end

    Sidekiq::Enterprise::Crypto::SCHEMES[true] = Sidekiq::Enterprise::Crypto::Default.new do
      OpenSSL::Cipher.new("aes-256-cbc").random_key
    end
    job["encrypt"] = true
    assert_equal job["args"], orig
    mid.call(nil, job, nil, nil) do
      refute_equal job["args"], orig
    end
    refute_equal job["args"], orig
  end

  class SecretWorker
    include Sidekiq::Worker
    sidekiq_options encrypt: true

    def perform(*args)
      p args
    end
  end

  it "has middleware which handles the entire job round trip" do

    Sidekiq::Enterprise::Crypto.enable(active_version: 1) do |v|
      OpenSSL::Cipher.new("aes-256-cbc").random_key
    end

    q = Sidekiq::Queue.new
    q.clear
    orig = ["clear", "open", { "ssn" => "123-45-6789" }]
    args = orig.dup
    jid = SecretWorker.set(encrypt: false).perform_async(*args)
    job = q.first.item
    assert job
    assert_equal job["jid"], jid
    assert_equal orig, job["args"]
    q.clear

    mid = Sidekiq::Enterprise::Crypto::Server.new
    mid.call(nil, job, nil) do
      assert_equal job["args"], orig
    end

    jid = SecretWorker.perform_async(*args)
    job = q.first.item

    assert job
    assert_equal jid, job["jid"]
    assert_equal 3, job["args"].size
    assert_equal 172, job["args"][2].size
    refute_equal orig, job["args"]

    mid.call(nil, job, nil) do
      assert_equal job["args"], orig
    end

    # Verify we don't double encrypt the last argument
    mid = Sidekiq::Enterprise::Crypto::Client.new
    mid.call(nil, job, nil, nil) do
      assert_equal 172, job["args"][2].size
    end
  end

end
