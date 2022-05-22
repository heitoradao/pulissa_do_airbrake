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
        menu_option = "You have 2 options:"
        load_projects = "Load Projects"
        select_project = "Select Project or Projects"

        description_entry_number = "Select a option number:"

        puts menu_introduction
        puts menu_option

        select_option([load_projects, select_project])
        puts entry_number

        entry_number = gets.chomp
        entry_number
      end

      private

      def select_option(options: [])
        options.each_with_index do |option, option_number|
          puts "#{option_number++} ) #{option}"
        end
      end
    end
  end
end
