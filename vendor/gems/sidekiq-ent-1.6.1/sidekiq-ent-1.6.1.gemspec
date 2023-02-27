# -*- encoding: utf-8 -*-
# stub: sidekiq-ent 1.6.1 ruby lib

Gem::Specification.new do |s|
  s.name = "sidekiq-ent"
  s.version = "1.6.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.metadata = { "allowed_push_host" => "https://enterprise.contribsys.com" } if s.respond_to? :metadata=
  s.require_paths = ["lib"]
  s.authors = ["Mike Perham"]
  s.date = "2017-09-14"
  s.email = ["mike@contribsys.com"]
  s.executables = ["sidekiqswarm"]
  s.files = ["bin/sidekiqswarm"]
  s.homepage = "http://sidekiq.org"
  s.licenses = ["Commercial"]
  s.rubygems_version = "2.4.5.2"
  s.summary = "Sidekiq Enterprise"

  s.installed_by_version = "2.4.5.2" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<sidekiq>, [">= 4.2.9"])
      s.add_runtime_dependency(%q<sidekiq-pro>, [">= 3.5.0"])
      s.add_development_dependency(%q<bundler>, [">= 0"])
      s.add_development_dependency(%q<rake>, [">= 0"])
      s.add_development_dependency(%q<minitest>, [">= 0"])
      s.add_development_dependency(%q<net-http-server>, [">= 0"])
      s.add_development_dependency(%q<gserver>, [">= 0"])
      s.add_development_dependency(%q<statsd-ruby>, [">= 0"])
    else
      s.add_dependency(%q<sidekiq>, [">= 4.2.9"])
      s.add_dependency(%q<sidekiq-pro>, [">= 3.5.0"])
      s.add_dependency(%q<bundler>, [">= 0"])
      s.add_dependency(%q<rake>, [">= 0"])
      s.add_dependency(%q<minitest>, [">= 0"])
      s.add_dependency(%q<net-http-server>, [">= 0"])
      s.add_dependency(%q<gserver>, [">= 0"])
      s.add_dependency(%q<statsd-ruby>, [">= 0"])
    end
  else
    s.add_dependency(%q<sidekiq>, [">= 4.2.9"])
    s.add_dependency(%q<sidekiq-pro>, [">= 3.5.0"])
    s.add_dependency(%q<bundler>, [">= 0"])
    s.add_dependency(%q<rake>, [">= 0"])
    s.add_dependency(%q<minitest>, [">= 0"])
    s.add_dependency(%q<net-http-server>, [">= 0"])
    s.add_dependency(%q<gserver>, [">= 0"])
    s.add_dependency(%q<statsd-ruby>, [">= 0"])
  end
end
