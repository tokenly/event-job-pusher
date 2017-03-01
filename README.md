Reads from a beanstalk queue, calls stats API and slack notifications.  Supports Keen.IO, Mixpanel and Slack.

## Requirements

Requires a running beanstalkd http://kr.github.io/beanstalkd/

## Installation

```
git clone https://github.com/tokenly/event-job-pusher
cd event-job-pusher/bin
npm install
```

## Usage

`BEANSTALK_HOST=127.0.0.1 node pusher.js`
