module Ui
  class Main
    class << self
      def introduction
        project_resume = 
         %{
           This scripts fetches data errors from airbrake, 
            and permits you to filter them by arbitrary 
             word included in paths of backtrace, 
                   or even in git-blame.
          }

        project_emoji = %{
                         __  _.-"` `'-.
                        /||\\'._ __{}_(
                        ||||  |'--.__\\
                        |  L.(   ^_\\^
                        \\ .-' |   _ |
                        | |   )\\___/
                        |  \\-'`:._]
                        \\__/;      '-.
        }

        puts project_resume
        puts project_emoji
      end

      def menu
        menu_introduction = "Pulisse Menu"
        load_projects = "Load Projects"
        select_project = "Select Project or Projects"
        program_exit = "Exit"

        options = [load_projects, select_project, program_exit]
        menu_option = "You have #{options.size} options:"

        description_entry_number = "Select a option number:"

        puts "\n"
        puts menu_introduction
        puts menu_option

        select_option(options: options)
        puts description_entry_number

        option_number = gets.chomp.to_i
        option_number
      end

      def presentation_of_option(option)
        case
        when option == 1
          load_projets_presentation
        when option == 2
          puts "Working on it. Thank U"
        when option == 3
          puts "Iuiuiuiuiu ..."
        else
          puts "Invalid Option"
        end
      end

      def load_projets_presentation
        projects = charge_projects

        list_with_id_and_name =
          projects.map { |project| project.slice('id', 'name') }

        list_projects(list_with_id_and_name)
      end

      private

      def select_option(options: [])
        options.each_with_index do |option, option_number|
          puts "#{option_number + 1} ) #{option}"
        end
      end

      def list_projects(projects)
        projects.each do |project|
          puts "- Project: #{project["name"]} - ID: #{project["id"]} -"
        end
      end

      def charge_projects
        if File.exist?('./data/projects.json')
          projects = File.read('./data/projects.json')
          JSON.parse(projects)
        else
          projects = ::Airbrake::API::Live.list_projects['projects']

          FileHandler::Main.generate_file(
            file_name: "projects",
            body: projects
          )

          projects
        end
      end
    end
  end
end
