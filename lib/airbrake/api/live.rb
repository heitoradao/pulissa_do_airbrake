module Airbrake
  class API
    class Live
      AIRBRAKE_URL = "https://api.airbrake.io/api/v4".freeze

      class << self
        def list_projects
          response = Faraday.get("#{AIRBRAKE_URL}/projects", airbrake_credentials)
          normalize_reponse(response)
        end

        def load_project_details(project_id)
          response = Faraday.get("#{AIRBRAKE_URL}/projects/#{project_id}", airbrake_credentials)
          normalize_reponse(response)
        end

        private

        def airbrake_credentials
          {
            key: ENV['AIRBRAKE_KEY'],
            content_type: 'application/json'
          }
        end

        def normalize_reponse(response)
          JSON.parse(response.body) || {}
        rescue JSON::ParserError => e
          puts e.message
        end
      end
    end
  end
end
