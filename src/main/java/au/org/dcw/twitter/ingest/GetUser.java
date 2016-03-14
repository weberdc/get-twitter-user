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
import twitter4j.conf.ConfigurationBuilder;

import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.imageio.ImageIO;


/**
 * Application that gets public timeline and profile for a Twitter user based on their screen name and stores the raw JSON responses into the {@link
 * #rootOutputDir} directory, with the same name as the {@link #screenName}.<p>
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

    protected void pauseIfNecessary(RateLimitStatus status) {
        if (status == null)
            return;

        int secondsUntilReset = status.getSecondsUntilReset();
        int callsRemaining = status.getRemaining();
        if (secondsUntilReset < 10 || callsRemaining < 10) {
            int untilReset = status.getSecondsUntilReset() + 5;
            System.out.println("Rate limit reached. Waiting " + untilReset + " seconds starting at " + Instant.now() + "...");
            try {
                Thread.sleep(untilReset * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Resuming...");
        }
    }

    /**
     * Usage: java au.org.dcw.twitter.ingest.GetUser [options] Options: -c, --credentials Properties file with Twitter OAuth credentials Default:
     * ./twitter.properties -f, --include-favourite-media Include media from favourite tweets Default: false -i, --include-media Include media from tweets
     * Default: false -o, --output Directory to which to write output Default: ./output -s, --screen-name Twitter screen name -u, --user-id Twitter user ID
     * -debug Debug mode Default: false
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
     * Runs the app, collecting profile, tweets and favourites for the user {@link #screenName} from twitter.com and writing them out as raw JSON to {@link
     * #rootOutputDir}.
     *
     * @throws IOException if there's a problem talking to Twitter or writing JSON out.
     */
    public void run() throws IOException {
        if (screenName != null && screenName.startsWith("@")) {
            screenName = screenName.substring(1);
        }

        System.out.println("== Configuration ==");
        System.out.println("screenName: " + screenName);
        System.out.println("userID: " + userID);
        System.out.println("identity: " + credentialsFile);
        System.out.println("media: " + includeMedia);
        System.out.println("fave media: " + includeFaveMedia);
        System.out.println("debug: " + debug);
        System.out.println();

        // TODO find a better name than credentials, given it might contain proxy info
        Properties credentials = loadCredentials(credentialsFile);

        ConfigurationBuilder conf = new ConfigurationBuilder();
        conf.setJSONStoreEnabled(true)
            .setDebugEnabled(debug)
            .setOAuthConsumerKey(credentials.getProperty("oauth.consumerKey"))
            .setOAuthConsumerSecret(credentials.getProperty("oauth.consumerSecret"))
            .setOAuthAccessToken(credentials.getProperty("oauth.accessToken"))
            .setOAuthAccessTokenSecret(credentials.getProperty("oauth.accessTokenSecret"));

        if (credentials.containsKey("http.proxyHost")) {
            conf.setHttpProxyHost(credentials.getProperty("http.proxyHost"))
                .setHttpProxyPort(Integer.parseInt(credentials.getProperty("http.proxyPort")))
                .setHttpProxyUser(credentials.getProperty("http.proxyUser"))
                .setHttpProxyPassword(credentials.getProperty("http.proxyPassword"));
        }

        Twitter twitter = new TwitterFactory(conf.build()).getInstance();
        twitter.addRateLimitStatusListener(rateLimitStatusListener);

        String name = screenName == null ? "user " + userID : "@" + screenName;
        System.out.println("Saving profile and timeline for " + name + ".");

        try {
            // Determine the user's screen name
            User user = fetchProfile(twitter);

            if (user.isProtected()) {
                System.out.println("User @" + screenName + " is protected.");
                System.exit(0);
            }

            // Use that to set up the output directories
            String userDir = rootOutputDir + "/" + screenName;
            String statusesDir = userDir + "/tweets";
            String favesDir = userDir + "/favourites";
            String mediaDir = userDir + "/media";
            new File(rootOutputDir).mkdirs();
            new File(userDir).mkdirs();
            new File(statusesDir).mkdirs();
            new File(favesDir).mkdirs();
            new File(mediaDir).mkdirs();

            // Write out the user's profile
            saveJSON(TwitterObjectFactory.getRawJSON(user), userDir + "/profile.json");

            Map<Long, Set<String>> mediaToFetch = new TreeMap<>();
            Map<Long, Set<String>> urlsMentioned = new TreeMap<>();

            // Retrieve and store status updates, as many as possible, one per file
            int tweetCount = fetchTweets(twitter, statusesDir, urlsMentioned, mediaToFetch);

            // Retrieve favourites
            int faveCount = fetchFavourites(twitter, favesDir, urlsMentioned, mediaToFetch);

            // Write out the mentioned URLs
            ObjectMapper json = new ObjectMapper();
            saveJSON(json.writeValueAsString(urlsMentioned), userDir + "/urls_mentioned.json");

            // Retrieve media
            int mediaCount = fetchMedia(mediaToFetch, mediaDir);

            // Report findings
            System.out.printf("\nUser @%s (%s)\n", screenName, userID.toString());
            System.out.println(" - tweets: " + tweetCount);
            System.out.println(" - favourites: " + faveCount);
            System.out.println(" - media: " + mediaCount);
            System.out.println("Written to " + userDir);

        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.out.println("Failed to store JSON: " + ioe.getMessage());
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to get timeline: " + te.getMessage());
            System.exit(-1);
        }
    }

    private int fetchMedia(Map<Long, Set<String>> mediaUrls, String mediaDir) {
        int fetched = 0;
        int tweetCount = 1;
        for (Map.Entry<Long, Set<String>> mediaUrl : mediaUrls.entrySet()) {
            Long id = mediaUrl.getKey();
            Set<String> urls = mediaUrl.getValue();
            int mediaCount = 1;
            for (String urlStr : urls) {
                urlStr = tweak(urlStr);
                System.out.printf("GET MEDIA %d/%d FROM %s ...", tweetCount++, mediaUrls.size(), urlStr);
                String ext = urlStr.substring(urlStr.lastIndexOf('.') + 1);
                String filename = id + "-" + (mediaCount++);
                try {
                    URL url = new URL(urlStr);
                    BufferedImage bi = ImageIO.read(url);
                    ImageIO.write(bi, ext, new File(mediaDir + "/" + filename + "." + ext));
                    fetched++;
                    System.out.println(" SUCCESS");
                } catch (IllegalArgumentException | IOException e) {
                    System.out.println(" FAIL - Skipping");
                }
            }
        }
        return fetched;
    }

    private String tweak(String urlStr) {
        if (urlStr.matches("^https?\\:\\/\\/imgur.com\\/")) {
            // e.g. "https://imgur.com/gallery/vLPhaca" to "https://i.imgur.com/vLPhaca.gif"
            String imgId = urlStr.substring(urlStr.lastIndexOf("/") + 1);
            urlStr = "https://i.imgur.com/download/" + imgId;
        }
        return urlStr;
    }

    private int fetchTweets(Twitter twitter, String statusesDir, Map<Long, Set<String>> urlsMentioned, Map<Long, Set<String>> mediaToFetch) throws TwitterException, IOException {
        int i = 1, tweetCount = 0;
        List<Status> tweets = twitter.getUserTimeline(screenName, new Paging(i++, TWEET_BATCH_SIZE));
        while (tweets.size() > 0) {
            pauseIfNecessary(tweets.get(0).getRateLimitStatus());
            processTweets(statusesDir, urlsMentioned, mediaToFetch, tweets, includeMedia);
            tweetCount += tweets.size();
            System.out.println(tweetCount + " tweets retrieved...");
            tweets = twitter.getUserTimeline(screenName, new Paging(i++, TWEET_BATCH_SIZE));
        }
        return tweetCount;
    }

    private int fetchFavourites(Twitter twitter, String favesDir, Map<Long, Set<String>> urlsMentioned, Map<Long, Set<String>> mediaToFetch) throws TwitterException, IOException {
        int i = 1;
        int faveCount = 0;
        List<Status> faves = twitter.getFavorites(screenName, new Paging(i++, TWEET_BATCH_SIZE));
        while (faves.size() > 0) {
            pauseIfNecessary(faves.get(0).getRateLimitStatus());
            processTweets(favesDir, urlsMentioned, mediaToFetch, faves, includeFaveMedia);
            faveCount += faves.size();
            System.out.println(faveCount + " favourite tweets retrieved...");
            faves = twitter.getFavorites(screenName, new Paging(i++, TWEET_BATCH_SIZE));
        }
        return faveCount;
    }

    private void processTweets(String favesDir, Map<Long, Set<String>> urlsMentioned, Map<Long, Set<String>> mediaToFetch, List<Status> faves,
                               boolean inclMediaUrls) throws IOException {
        for (Status tweet : faves) {
            String rawJSON = TwitterObjectFactory.getRawJSON(tweet);
            String fileName = favesDir + "/" + tweet.getId() + ".json";
            saveJSON(rawJSON, fileName);

            associateURLsWithTweet(urlsMentioned, tweet, collectMentionedURLs(tweet));
            if (inclMediaUrls) {
                collectMediaURLs(mediaToFetch, tweet);
            }
        }
    }

    private void collectMediaURLs(Map<Long, Set<String>> mediaToFetch, Status status) {
        Set<String> urls = collectMentionedURLs(status);

        if (status.getMediaEntities().length > 0) {
            for (MediaEntity entity : status.getMediaEntities()) {
                urls.add(entity.getMediaURLHttps());
            }
        }

        if (status.getExtendedMediaEntities().length > 0) {
            for (ExtendedMediaEntity entity : status.getExtendedMediaEntities()) {
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

        associateURLsWithTweet(mediaToFetch, status, urls);
    }

    private void associateURLsWithTweet(Map<Long, Set<String>> mediaToFetch, Status status, Set<String> urls) {
        for (String url : urls) {
            mediaToFetch.putIfAbsent(status.getId(), new TreeSet<String>());
            mediaToFetch.get(status.getId()).add(url);
        }
    }

    private Set<String> collectMentionedURLs(Status status) {
        Set<String> urls = new TreeSet<>();
        if (status.getURLEntities().length > 0) {
            for (URLEntity entity : status.getURLEntities()) {
                urls.add(entity.getExpandedURL());
            }
        }
        return urls;
    }

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
