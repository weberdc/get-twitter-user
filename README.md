# Get Twitter User

Author: **Derek Weber** (with many thanks to [http://twitter4j.org]() examples)

Last updated: **2016-03-12**

Simple app to retrieve the profile, tweets (as many as possible), and favourited
tweets for a given user, specified by screen name or Twitter user identifier.

Requirements:
 + Java 1.8
 + [twitter4j-core](http://twitter4j.org) (Apache 2.0 licence)
   + depends on [JSON](http://json.org) ([JSON licence](http://www.json.org/license.html))
 + [FasterXML](http://wiki.fasterxml.com/JacksonHome) (Apache 2.0 licence)
 + [jcommander](http://jcommander.org) (Apache 2.0 licence)

Built with [Gradle 2.11](http://gradle.org).

Twitter OAuth credentials must be available in a properties file based on the
provided `twitter.properties-template` in the project's root directory. Support
for running behind a proxy is also there, based on Twitter4J's support. For
further information see [http://twitter4j.org/en/configuration.html]().

## To Build

By running

`$ gradle installDist`

you will create an installable copy of the app in `PROJECT_ROOT/build/get-twitter-user`.

## Usage
From within `PROJECT_ROOT/build/install/get-twitter-user`:
```
Usage: bin/get-twitter-user[.bat] [options]
  Options:
     -c, --credentials
         Properties file with Twitter OAuth credential
         Default: ./twitter.properties
     -f, --include-favourite-media
         Include media from favourite tweets
         Default: false
     -i, --include-media
         Include media from tweets
         Default: false
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

Running the app with a given Twitter screen name, e.g. `weberdc`, will create
a directory `output/weberdc` and download:

 + `@weberdc`'s profile to `output/weberdc/profile.json`
 + `@weberdc`'s tweets, one per file, to `output/weberdc/statuses/`
 + `@weberdc`'s favourited tweets, one per file, to `output/weberdc/favourites`
 + `@weberdc`'s images and other media mentioned, to `output/weberdc/media`

Attempts have been made to account for Twitter's rate limits, so at times the
app will pause, waiting until the rate limit has refreshed. It reports how long
it will wait.