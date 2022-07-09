module Airbrake
  class API
    class Mock
      class << self
        def list_projects
          list = { 'projects' => [] }

          (1..10).each do
            list['projects'] << generate_project
          end

          list
        end

        private

        def generate_project
          {
            'id'=> Faker::Number.number(digits: 6),
            'name'=> Faker::Name.first_name ,
            'apiKey'=> Faker::Crypto.sha1,
            'language'=> Faker::ProgrammingLanguage.name,
            'resolveErrorsOnDeploy'=>true,
            'rateLimited'=>false,
            'paymentFailedAt'=>nil,
            'delinquent'=>false,
            'restricted'=>false,
            'isFirstProject'=>false,
            'demoModeUntilDate'=>nil,
            'apdexThreshold'=>rand(1..80),
            'notifier'=>{'name'=>'airbrake', 'version'=>'0.00.0', 'url'=>'https://github.com/airbrake'},
            'attributes'=>nil,
            'noticeTotalCount'=>rand(1..1000000),
            'deployCount'=>rand(1..60),
            'groupResolvedCount'=>rand(1..200),
            'groupUnresolvedCount'=>rand(1..200),
            'firstNoticeReceivedAt'=>'2020-08-10T20:05:48.615Z',
            'lastNoticeAt'=>'2020-08-10T20:05:48.615Z',
            'lastRequestAt'=>'2020-08-10T20:05:48.615Z',
            'lastDeployAt'=>'2020-08-10T20:05:48.615Z',
            'createdAt'=>'2020-08-10T20:05:48.615Z'
          }
        end
      end
    end
  end
end
