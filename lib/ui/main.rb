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
    end
  end
end
