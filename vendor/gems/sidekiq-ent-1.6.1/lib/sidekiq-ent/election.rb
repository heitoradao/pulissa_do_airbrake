Sidekiq.configure_server do |config|
  config.on(:startup) do
    require 'sidekiq-ent/senate'
    senate = Sidekiq::Senate.instance
    senate.start(Sidekiq::CLI.instance)

    Sidekiq::CLI::PROCTITLES << proc { 'leader' if senate.leader? }
  end

  config.on(:shutdown) do
    senate = Sidekiq::Senate.instance
    senate.terminate
  end
end
