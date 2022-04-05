# 🚨 Pulissa do Airbrake 🚨

This scripts fetches data errors from [airbrake](https://airbrake.io/),
and permits you to filter them by arbitrary word included in paths of backtrace, or even in git-blame.


## Setup

Open your airbrake profile page, copy the API token and paste inside `.env` file.

![access the profile](https://github.com/heitoradao/pulissa_do_airbrake/blob/main/docs/profile.png?raw=true)

![get api key](https://github.com/heitoradao/pulissa_do_airbrake/blob/main/docs/api-key.png?raw=true)


## Setup (part 2)

Insert the api key inside `.env` file.

Write the PROJECT_ID that you are monitoring in the appropriate place.

Optionally, insert the path to your git repo, so this scripts can fetch git-blame info on them.


## Using

Open `list-groups.rb` in your editor and see what it does.

Run `list-groups.rb` from your terminal.

It will fetch data from airbrake, parse them, and open a pry's terminal where you can query'n'hack things.

In the variable `groups` you will have the first 100 error groups from airbrake.
In `my_fault`, there is a filtered list.


## TODO

- [ ] Fetch info from more then 1 project.
- [ ] Create a separate script to update the cached json file.
- [ ] Create a cronjob to call the script that update cached json.
- [ ] Notify user when new errors that match a criteria where found,
