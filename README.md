# 🚨 Pulissa do Airbrake 🚨

This scripts fetches data errors from [airbrake](https://airbrake.io/),
and permits you to filter them by arbitrary word included in paths of backtrace, or even in git-blame.

## Setup

Open your airbrake profile page, copy the API token and paste inside `.env` file.

![access the profile](https://github.com/heitoradao/pulissa_do_airbrake/blob/main/docs/profile.png?raw=true)

![get api key](https://github.com/heitoradao/pulissa_do_airbrake/blob/main/docs/api-key.png?raw=true)

Insert the api key inside `.env` file.

Write the PROJECT_ID.

Optionally, insert the path to your git repo, so this scripts can fetch git-blame info on them.
