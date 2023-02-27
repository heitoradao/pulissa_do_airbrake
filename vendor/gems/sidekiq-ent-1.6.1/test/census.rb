require_relative "helper"

class TestCensus < Minitest::Test
  def setup
    @env = Sidekiq.options[:environment]
  end

  def teardown
    Sidekiq.options[:environment] = @env
  end

  def test_parameterize
    expected = "v=1&tag=my%2Bapp&rver=#{RUBY_VERSION}&sver=#{Sidekiq::VERSION}&ever=#{Sidekiq::Enterprise::VERSION}&threads=0&processes=0&jobs=0&user=facebeef"
    assert_equal expected, census.parameterize
  end

  def test_pause
    prev, ENV['TZ'] = ENV['TZ'], 'US/Pacific'
    begin
      tomorrow = (Time.now + 86400)
      window = Time.local(tomorrow.year, tomorrow.mon, tomorrow.day, 3, 30, 0)
      assert_in_delta window.to_i - Time.now.to_i, census.pause, 3600
    ensure
      ENV['TZ'] = prev
    end
  end

  def test_properties
    assert_equal "facebeef", census.user
    assert_equal 15, census.minute
    assert_equal [0, 0, 0], census.scale_metrics
    assert_equal true, census.valid?
  end

  def test_development
    refute census("environment" => "development").start
  end

  def test_bad_credentials
    assert census.valid?
    refute census("credentials" => "yuck").valid?
  end

  def default
    {
      'credentials' => "facebeef:cafebabe",
      'environment' => 'production',
      'tag' => 'my+app',
    }
  end

  def census(params={})
    config = default.merge(params)
    @census = begin
      Bundler.settings["enterprise.contribsys.com"] = config['credentials']
      Sidekiq.options[:environment] = config['environment']
      Sidekiq.options[:tag] = config['tag']
      Sidekiq::Enterprise::Census.new
    end
  end
end
