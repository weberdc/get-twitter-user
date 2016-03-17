/*
 * Copyright 2016 Derek Weber
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package au.org.dcw.twitter.ingest;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;

import twitter4j.ExtendedMediaEntity;
import twitter4j.MediaEntity;
import twitter4j.Paging;
import twitter4j.RateLimitStatus;
import twitter4j.RateLimitStatusEvent;
import twitter4j.RateLimitStatusListener;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.imageio.ImageIO;


/**
 * Application that gets public timeline and profile for a Twitter user based
 * on their screen name and stores the raw JSON responses into the
 * {@link #rootOutputDir} directory, with the same name as the {@link #screenName}.<p>
 *
 * Borrows heavily from the <a href="http://twitter4j.org">Twitter4j</a> example:
 *
 * @see <a href="https://github.com/yusuke/twitter4j/blob/master/twitter4j-examples/src/main/java/twitter4j/examples/json/SaveRawJSON.java">SaveRawJSON.java</a></a>
 */
public final class GetUser {

    public static final int TWEET_BATCH_SIZE = 200;

    @Parameter(names = {"-s", "--screen-name"}, description = "Twitter screen name")
    private String screenName;

    @Parameter(names = {"-u", "--user-id"}, description = "Twitter user ID (@ symbol optional)")
    private Long userID;

    @Parameter(names = {"-o", "--output"}, description = "Directory to which to write output")
    private String rootOutputDir = "./output";

    @Parameter(names = {"-c", "--credentials"},
        description = "Properties file with Twitter OAuth credentials")
    private String credentialsFile = "./twitter.properties";

    @Parameter(names = "-debug", description = "Debug mode")
    private boolean debug = false;

    @Parameter(names = {"-i", "--include-media"}, description = "Include media from tweets")
    private boolean includeMedia = false;

    @Parameter(names = {"-f", "--include-favourite-media"}, description = "Include media from favourite tweets")
    private boolean includeFaveMedia = false;

    /**
     * Listener to pay attention to when the Twitter's rate limit is being approached or breached.
     */
    RateLimitStatusListener rateLimitStatusListener = new RateLimitStatusListener() {
        @Override
        public void onRateLimitStatus(RateLimitStatusEvent event) {
            pauseIfNecessary(event.getRateLimitStatus());
        }

        @Override
        public void onRateLimitReached(RateLimitStatusEvent event) {
            pauseIfNecessary(event.getRateLimitStatus());
        }
    };

    /**
     * If the provided {@link RateLimitStatus} indicates that we are about to break the rate
     * limit, in terms of number of calls or time window, then sleep for the rest of the period.
     *
     * @param status The current status of the our calls to Twitter
     */
    protected void pauseIfNecessary(RateLimitStatus status) {
        if (status == null)
            return;

        int secondsUntilReset = status.getSecondsUntilReset();
        int callsRemaining = status.getRemaining();
        if (secondsUntilReset < 10 || callsRemaining < 10) {
            int untilReset = status.getSecondsUntilReset() + 5;
            System.out.println("Rate limit reached. Waiting " + untilReset + " seconds starting at " + new Date() + "...");
            try {
                Thread.sleep(untilReset * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Resuming...");
        }
    }

    /**
     * Usage: java au.org.dcw.twitter.ingest.GetUser [options]
     * Options:
     *   -c, --credentials
     *      Properties file with Twitter OAuth credentials
     *      Default: ./twitter.properties
     *   -f, --include-favourite-media
     *      Include media from favourite tweets
     *      Default: false
     *   -i, --include-media
     *      Include media from tweets
     *      Default: false
     *   -o, --output
     *      Directory to which to write output
     *      Default: ./output
     *   -s, --screen-name
     *      Twitter screen name
     *   -u, --user-id
     *      Twitter user ID
     *   -debug
     *      Debug mode
     *      Default: false
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) throws IOException {
        GetUser theApp = new GetUser();

        JCommander argsParser = new JCommander(theApp, args);

        if (!checkFieldsOf(theApp)) {
            StringBuilder sb = new StringBuilder();
            argsParser.usage(sb);
            System.out.println(sb.toString());
            System.exit(-1);
        }

        theApp.run();
    }

    /**
     * Checks to see if commandline argument constraints have been met.
     *
     * @param app the app the fields of which to check
     */
    private static boolean checkFieldsOf(GetUser app) {
        if (app.screenName == null && app.userID == null) {
            System.out.println("Error: screen name or twitter ID must be supplied");
            return false;
        }
        return true;
    }

    /**
     * Runs the app, collecting profile, tweets and favourites for the user {@link #screenName}
     * from twitter.com and writing them out as raw JSON to {@link #rootOutputDir}.
     *
     * @throws IOException if there's a problem talking to Twitter or writing JSON out.
     */
    public void run() throws IOException {
        // clean screenName of a leading @ symbol
        if (screenName != null && screenName.startsWith("@")) {
            screenName = screenName.substring(1);
        }

        reportConfiguration();

        Twitter twitter = new TwitterFactory(buildTwitterConfiguration()).getInstance();
        twitter.addRateLimitStatusListener(rateLimitStatusListener);

        String name = screenName == null ? "user " + userID : "@" + screenName;
        System.out.println("Saving profile and timeline for " + name + ".");

        try {
            // Determine the user's screen name
            User user = fetchProfile(twitter);

            // Use that to set up the output directories
            String userDir = rootOutputDir + "/" + screenName;
            new File(userDir).mkdirs();

            // Write out the user's profile
            saveJSON(TwitterObjectFactory.getRawJSON(user), userDir + "/profile.json");

            if (user.isProtected()) {
                System.out.println("User @" + screenName + " is protected.");
                System.exit(0);
            }

            Map<Long, Set<String>> mediaToFetch = new TreeMap<>();
            Map<Long, Set<String>> urlsMentioned = new TreeMap<>();

            // Retrieve and store status updates, as many as possible, one per file
            int tweetCount = fetchTweets(twitter, userDir + "/tweets", urlsMentioned, mediaToFetch);

            // Retrieve favourites
            int faveCount = fetchFavourites(twitter, userDir + "/favourites", urlsMentioned, mediaToFetch);

            // Write out the mentioned URLs
            stripEmptyUrlSets(urlsMentioned);
            if (! urlsMentioned.isEmpty()) {
                ObjectMapper json = new ObjectMapper();
                saveJSON(json.writeValueAsString(urlsMentioned), userDir + "/urls_mentioned.json");
            }

            // Retrieve media
            int mediaCount = fetchMedia(mediaToFetch, userDir + "/media");

            // Report findings
            reportFindings(userDir, tweetCount, faveCount, mediaCount);

        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.out.println("Failed to store JSON: " + ioe.getMessage());
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to get timeline: " + te.getMessage());
            System.exit(-1);
        }
    }

    private void stripEmptyUrlSets(Map<Long, Set<String>> urlsMentioned) {
        Set<Long> toRemove = urlsMentioned.keySet().stream()
                                          .filter (k -> urlsMentioned.get(k).isEmpty())
                                          .collect(Collectors.toSet());
        for (Long id : toRemove) {
            urlsMentioned.remove(id);
        }
    }

    /**
     * Reports what was found.
     *
     * @param userDir    the directory to which results have been written
     * @param tweetCount the number of the user's tweets retrieved
     * @param faveCount  the number of the user's favourites retrieved
     * @param mediaCount the number of URLs mentioned which referred to media which were successfully downloaded
     */
    private void reportFindings(String userDir, int tweetCount, int faveCount, int mediaCount) {
        System.out.printf("\nUser @%s (%s)\n", screenName, userID.toString());
        System.out.println(" - tweets: " + tweetCount);
        System.out.println(" - favourites: " + faveCount);
        System.out.println(" - media: " + mediaCount);
        System.out.println("Written to " + userDir);
    }

    /**
     * An introductory report to stdout, highlighting the configuration to be used.
     */
    private void reportConfiguration() {
        System.out.println("== Configuration ==");
        System.out.println("screenName: " + screenName);
        System.out.println("userID: " + userID);
        System.out.println("identity: " + credentialsFile);
        System.out.println("media: " + includeMedia);
        System.out.println("fave media: " + includeFaveMedia);
        System.out.println("debug: " + debug);
        System.out.println();
    }

    /**
     * Builds the {@link Configuration} object with which to connect to Twitter, including
     * credentials and proxy information if it's specified.
     *
     * @return a Twitter4j {@link Configuration} object
     * @throws IOException if there's an error loading the application's {@link #credentialsFile}.
     */
    private Configuration buildTwitterConfiguration() throws IOException {
        // TODO find a better name than credentials, given it might contain proxy info
        Properties credentials = loadCredentials(credentialsFile);

        ConfigurationBuilder conf = new ConfigurationBuilder();
        conf.setJSONStoreEnabled(true)
            .setDebugEnabled(debug)
            .setOAuthConsumerKey(credentials.getProperty("oauth.consumerKey"))
            .setOAuthConsumerSecret(credentials.getProperty("oauth.consumerSecret"))
            .setOAuthAccessToken(credentials.getProperty("oauth.accessToken"))
            .setOAuthAccessTokenSecret(credentials.getProperty("oauth.accessTokenSecret"));

        Properties proxies = loadProxyProperties();
        if (proxies.containsKey("http.proxyHost")) {
            conf.setHttpProxyHost(proxies.getProperty("http.proxyHost"))
                .setHttpProxyPort(Integer.parseInt(proxies.getProperty("http.proxyPort")))
                .setHttpProxyUser(proxies.getProperty("http.proxyUser"))
                .setHttpProxyPassword(proxies.getProperty("http.proxyPassword"));
        }

        return conf.build();
    }

    /**
     * Fetches as many of the {@code mediaUrls} as possible to {@code mediaDir}.
     *
     * @param mediaUrls the URLs of potential media files
     * @param mediaDir  the directory to which to write the media files
     * @return the number of URLs that referred to media which were successfully downloaded
     */
    private int fetchMedia(Map<Long, Set<String>> mediaUrls, String mediaDir) {
        new File(mediaDir).mkdirs();

        int fetched = 0;
        int tweetCount = 0;
        for (Map.Entry<Long, Set<String>> mediaUrl : mediaUrls.entrySet()) {
            Long id = mediaUrl.getKey();
            Set<String> urls = mediaUrl.getValue();
            tweetCount++;
            int mediaCount = 1;
            for (String urlStr : urls) {
                urlStr = tweak(urlStr);
                System.out.printf("GET MEDIA %d/%d FROM %s ...", tweetCount, mediaUrls.size(), urlStr);
                String ext = urlStr.substring(urlStr.lastIndexOf('.') + 1);
                String filename = id + "-" + (mediaCount++);
                try {
                    URL url = new URL(urlStr);
                    BufferedImage bi = ImageIO.read(url);
                    ImageIO.write(bi, ext, new File(mediaDir + "/" + filename + "." + ext));
                    fetched++;
                    System.out.println(" SUCCESS");
                } catch (IllegalArgumentException | IOException e) {
                    System.out.println(" FAIL(" + e.getMessage() + ") - Skipping");
                }
            }
        }
        return fetched;
    }

    /**
     * Opportunity to modify known URLs to make it easier to acces the media to which they refer.
     *
     * @param urlStr the original URL string
     * @return a potentially modified URL string
     */
    private String tweak(String urlStr) {
        if (urlStr.matches("^https?\\:\\/\\/imgur.com\\/")) {
            // e.g. "https://imgur.com/gallery/vLPhaca" to "https://i.imgur.com/vLPhaca.gif"
            String imgId = urlStr.substring(urlStr.lastIndexOf("/") + 1);
            urlStr = "https://i.imgur.com/download/" + imgId;
        }
        return urlStr;
    }

    /**
     * Fetch the tweets of the user and save them to the {@code tweetsDir}. Also records URLs
     * mentioned, and specifically ones that refer to media.
     *
     * @param twitter       the Twitter API instance
     * @param tweetsDir     the directory to which to save tweets
     * @param urlsMentioned a map of tweet ID to a set of URLs mentioned to update
     * @param mediaToFetch  a map of tweet ID to a set of URLs to media mentioned to update
     * @return the number of tweets successfully retrieved and saved
     * @throws TwitterException if an error occurs talking to the Twitter API
     * @throws IOException if an error occurs talking to the network or the file system
     */
    private int fetchTweets(Twitter twitter, String tweetsDir, Map<Long, Set<String>> urlsMentioned,
                            Map<Long, Set<String>> mediaToFetch) throws TwitterException, IOException {
        new File(tweetsDir).mkdirs();

        int i = 1, tweetCount = 0;
        List<Status> tweets = twitter.getUserTimeline(screenName, new Paging(i++, TWEET_BATCH_SIZE));
        while (tweets.size() > 0) {
            pauseIfNecessary(tweets.get(0).getRateLimitStatus());
            processTweets(tweets, tweetsDir, urlsMentioned, mediaToFetch, includeMedia);
            tweetCount += tweets.size();
            System.out.println(tweetCount + " tweets retrieved...");
            tweets = twitter.getUserTimeline(screenName, new Paging(i++, TWEET_BATCH_SIZE));
        }
        return tweetCount;
    }

    /**
     * Fetch the favourite tweets of the user and save them to the {@code favesDir}. Also
     * records URLs mentioned, and specifically ones that refer to media.
     *
     * @param twitter       the Twitter API instance
     * @param favesDir      the directory to which to save favourited tweets
     * @param urlsMentioned a map of tweet ID to a set of URLs mentioned to update
     * @param mediaToFetch  a map of tweet ID to a set of URLs to media mentioned to update
     * @return the number of tweets successfully retrieved and saved
     * @throws TwitterException if an error occurs talking to the Twitter API
     * @throws IOException if an error occurs talking to the network or the file system
     */
    private int fetchFavourites(Twitter twitter, String favesDir, Map<Long, Set<String>> urlsMentioned,
                                Map<Long, Set<String>> mediaToFetch) throws TwitterException, IOException {
        new File(favesDir).mkdirs();

        int i = 1;
        int faveCount = 0;
        List<Status> faves = twitter.getFavorites(screenName, new Paging(i++, TWEET_BATCH_SIZE));
        while (faves.size() > 0) {
            pauseIfNecessary(faves.get(0).getRateLimitStatus());
            processTweets(faves, favesDir, urlsMentioned, mediaToFetch, includeFaveMedia);
            faveCount += faves.size();
            System.out.println(faveCount + " favourite tweets retrieved...");
            faves = twitter.getFavorites(screenName, new Paging(i++, TWEET_BATCH_SIZE));
        }
        return faveCount;
    }

    /**
     * Process the {@code tweets} and save them to the {@code tweetsDir}. Also
     * records URLs mentioned, and specifically ones that refer to media if
     * {@code inclMediaURLs} is true.
     *
     * @param tweets        the tweets to process
     * @param tweetsDir     the directory in which to store tweets
     * @param urlsMentioned a map of tweet ID to a set of URLs mentioned to update
     * @param mediaToFetch  a map of tweet ID to a set of URLs to media mentioned to update
     * @param inclMediaURLs if true, tweets will be examined for attached media
     * @throws IOException if an error occurs writing to the file system
     */
    private void processTweets(List<Status> tweets, String tweetsDir, Map<Long, Set<String>> urlsMentioned, Map<Long,
                               Set<String>> mediaToFetch, boolean inclMediaURLs)
        throws IOException {
        new File(tweetsDir).mkdirs(); // make sure

        for (Status tweet : tweets) {
            String rawJSON = TwitterObjectFactory.getRawJSON(tweet);
            String fileName = tweetsDir + "/" + tweet.getId() + ".json";
            saveJSON(rawJSON, fileName);

            associateURLsWithTweet(tweet, collectMentionedURLs(tweet), urlsMentioned);
            if (inclMediaURLs) {
                collectMediaURLs(tweet, mediaToFetch);
            }
        }
    }

    /**
     * Look for all URLs in the given Tweet in case they refer to media of some kind,
     * and add them to the {@code mediaURLs} map.
     *
     * @param tweet     the Tweet to examine
     * @param mediaURLs a map of Tweet IDs to a sets of URLs
     */
    private void collectMediaURLs(Status tweet, Map<Long, Set<String>> mediaURLs) {
        Set<String> urls = collectMentionedURLs(tweet);

        if (tweet.getMediaEntities().length > 0) {
            for (MediaEntity entity : tweet.getMediaEntities()) {
                urls.add(entity.getMediaURLHttps());
            }
        }

        if (tweet.getExtendedMediaEntities().length > 0) {
            for (ExtendedMediaEntity entity : tweet.getExtendedMediaEntities()) {
                switch (entity.getType()) {
                    case "video":
                        urls.add(entity.getVideoVariants()[0].getUrl());
                        break;
                    default:
                        urls.add(entity.getMediaURLHttps());
                        break;
                }
            }
        }

        associateURLsWithTweet(tweet, urls, mediaURLs);
    }

    /**
     * Convenience method to add a new URL to the set of URLs associated with the given
     * {@code tweet} in the given map.
     *
     * @param tweet     the source of the URLs
     * @param urls      the extracted URLs
     * @param mediaURLs a map of Long (Tweet ID) to set of Strings (URLs)
     */
    private void associateURLsWithTweet(Status tweet, Set<String> urls, Map<Long, Set<String>> mediaURLs) {
        mediaURLs.putIfAbsent(tweet.getId(), new TreeSet<String>());
        mediaURLs.get(tweet.getId()).addAll(urls);
    }

    /**
     * A convenience method to collect the URLs mentioned in {@code tweet} and return
     * them in a set.
     *
     * @param tweet a Tweet, perhaps containing URLs
     * @return the set of URL strings mentioned in the tweet
     */
    private Set<String> collectMentionedURLs(Status tweet) {
        Set<String> urls = new TreeSet<>();
        if (tweet.getURLEntities().length > 0) {
            for (URLEntity entity : tweet.getURLEntities()) {
                urls.add(entity.getExpandedURL());
            }
        }
        return urls;
    }

    /**
     * Fetches the profile for a given user using the Twitter API object and using
     * {@link #screenName} or {@link #userID} to do it. The field not used for lookup
     * is populated after the profile is found.
     *
     * @param twitter the Twitter API object
     * @return the Twitter4j {@link User profile} of the specified user
     * @throws TwitterException if an error occurs talking to Twitter
     */
    private User fetchProfile(Twitter twitter) throws TwitterException {
        User user = null;
        if (screenName != null) {
            user = twitter.showUser(screenName);
            userID = Long.valueOf(user.getId());
        } else if (userID != null) {
            user = twitter.showUser(userID.longValue());
            screenName = user.getScreenName();
        }
        if (user != null)
            pauseIfNecessary(user.getRateLimitStatus());
        return user;
    }

    /**
     * Loads the given {@code credentialsFile} from disk.
     *
     * @param credentialsFile the properties file with the Twitter credentials in it
     * @return A {@link Properties} map with the contents of credentialsFile
     * @throws IOException if there's a problem reading the credentialsFile.
     */
    private static Properties loadCredentials(String credentialsFile) throws IOException {
        Properties properties = new Properties();
        properties.load(Files.newBufferedReader(Paths.get(credentialsFile)));
        return properties;
    }

    private static Properties loadProxyProperties() {
        Properties properties = new Properties();
        String proxyFile = "./proxy.properties";
        if (new File(proxyFile).exists()) {
            boolean success = true;
            try (Reader fileReader = Files.newBufferedReader(Paths.get(proxyFile))) {
                properties.load(fileReader);
            } catch (IOException e) {
                System.err.println("Attempted and failed to load " + proxyFile + ": " + e.getMessage());
                success = false;
            }
            if (success && !properties.containsKey("http.proxyPassword")) {
                char[] password = System.console().readPassword("Please type in your proxy password: ");
                properties.setProperty("http.proxyPassword", new String(password));
            }
            properties.forEach((k, v) -> System.setProperty(k.toString(), v.toString()));
        }
        return properties;
    }

    /**
     * Writes the given {@code rawJSON} {@link String} to the specified file.
     *
     * @param rawJSON  the JSON String to persist
     * @param fileName the file (including path) to which to write the JSON
     * @throws IOException if there's a problem writing to the specified file
     */
    private static void saveJSON(String rawJSON, String fileName) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(fileName);
             OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
             BufferedWriter bw = new BufferedWriter(osw)) {
            bw.write(rawJSON);
            bw.flush();
        }
    }
}
