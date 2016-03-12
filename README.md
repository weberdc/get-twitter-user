# Get Twitter User App

Author: **Derek Weber** (with many thanks to http://Twitter4j.org examples)

Last updated: **2016-03-12**

Simple app to retrieve the profile, tweets (as many as possible), and favourited
tweets for a given user, specified by screen name or Twitter user identifier.

## Usage
```
Usage: bin/get-twitter-user[.bat] [options]
  Options:
     -c, --credentials
         Properties file with Twitter OAuth credential
         Default: ./twitter.properties
     -o, --output
         Directory to which to write output
         Default: ./output
     -s, --screen-name
         Twitter screen name
     -u, --user-id
         Twitter user ID
     -debug
         Debug mode
         Default: false
```
