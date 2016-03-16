# Get Twitter User

Author: **Derek Weber** (with many thanks to [http://twitter4j.org]() examples)

Last updated: **2016-03-17**

Simple app to retrieve the profile, tweets (as many as possible), and favourited
tweets for a given user, specified by screen name or Twitter user identifier.

Requirements:
 + Java Development Kit 1.8
 + [twitter4j-core](http://twitter4j.org) (Apache 2.0 licence)
   + depends on [JSON](http://json.org) ([JSON licence](http://www.json.org/license.html))
 + [FasterXML](http://wiki.fasterxml.com/JacksonHome) (Apache 2.0 licence)
 + [jcommander](http://jcommander.org) (Apache 2.0 licence)

Built with [Gradle 2.11](http://gradle.org).

## To Build

The Gradle wrapper has been included, so it's not necessary for you to have Gradle
installed - it will install itself as part of the build process. All that is required is
the Java Development Kit.

By running

`$ ./gradlew installDist` or `$ gradlew.bat installDist`

you will create an installable copy of the app in `PROJECT_ROOT/build/get-twitter-user`.

## Configuration

Twitter OAuth credentials must be available in a properties file based on the
provided `twitter.properties-template` in the project's root directory. Copy the
template file to a properties file (the default is `twitter.properties` in the same
directory), and edit it with your Twitter app credentials. For further information see
[http://twitter4j.org/en/configuration.html]().

If running the app behind a proxy or filewall, copy the `proxy.properties-template`
file to a file named `proxy.properties` and set the properties inside to your proxy
credentials. If you feel uncomfortable putting your proxy password in the file, leave
it commented and the app will ask for the password.

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