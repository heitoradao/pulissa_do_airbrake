# -*- encoding: utf-8 -*-
# stub: sidekiq-pro 3.7.1 ruby lib

Gem::Specification.new do |s|
  s.name = "sidekiq-pro"
  s.version = "3.7.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.metadata = { "allowed_push_host" => "https://gems.contribsys.com", "changelog_uri" => "https://github.com/mperham/sidekiq/blob/master/Pro-Changes.md", "documentation_uri" => "https://github.com/mperham/sidekiq/wiki", "wiki_uri" => "https://github.com/mperham/sidekiq/wiki" } if s.respond_to? :metadata=
  s.require_paths = ["lib"]
  s.authors = ["Mike Perham"]
  s.date = "2018-01-31"
  s.description = "Loads of additional functionality for Sidekiq"
  s.email = ["mike@contribsys.com"]
  s.homepage = "http://sidekiq.org"
  s.licenses = ["Commercial"]
  s.rubygems_version = "2.4.5.2"
  s.summary = "Black belt functionality for Sidekiq"

  s.installed_by_version = "2.4.5.2" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<sidekiq>, [">= 4.1.5"])
      s.add_development_dependency(%q<statsd-ruby>, [">= 0"])
      s.add_development_dependency(%q<dogstatsd-ruby>, [">= 0"])
    else
      s.add_dependency(%q<sidekiq>, [">= 4.1.5"])
      s.add_dependency(%q<statsd-ruby>, [">= 0"])
      s.add_dependency(%q<dogstatsd-ruby>, [">= 0"])
    end
  else
    s.add_dependency(%q<sidekiq>, [">= 4.1.5"])
    s.add_dependency(%q<statsd-ruby>, [">= 0"])
    s.add_dependency(%q<dogstatsd-ruby>, [">= 0"])
  end
end
