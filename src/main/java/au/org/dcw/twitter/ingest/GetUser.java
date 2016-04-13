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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import twitter4j.ExtendedMediaEntity;
import twitter4j.MediaEntity;
import twitter4j.Paging;
import twitter4j.RateLimitStatus;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
    public static final String API_FETCH_PROFILE = "/users/show/:id";
    public static final int MOVED_PERMANENTLY = 301;
    public static final int MOVED_TEMPORARILY = 302;
    public static final int TEMPORARY_REDIRECT = 307;
    private final CloseableHttpClient httpClient;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private long delay; // most recent delay for scheduled tasks
    private Twitter twitter;

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
     * Used collect the result of a call to the Twitter API and also its
     * raw JSON form, which is not available from other threads (and so must
     * be collected at the time the call to the API returns.
     *
     * @param <T> The Twitter4J type of the result from the API call
     */
    class Wrapper<T> {
        public T value;
        public String rawJSON;

        public Wrapper() {}
        public Wrapper(T value, String rawJSON) {
            this.value = value;
            this.rawJSON = rawJSON;
        }
    }

    /**
     * A task to fetch a user's Twitter profile which relies on {@link #screenName},
     * {@link #userID} and {@link #twitter}.
     */
    private final Callable<Wrapper<User>> fetchProfileTask = () -> {
        System.out.println("Fetching profile at " + new Date());
        Wrapper<User> user = new Wrapper();
        if (screenName != null) {
            user.value = twitter.showUser(screenName);
            userID = Long.valueOf(user.value.getId());
        } else if (userID != null) {
            user.value = twitter.showUser(userID.longValue());
            screenName = user.value.getScreenName();
        }
        if (user != null) {
            user.rawJSON = TwitterObjectFactory.getRawJSON(user.value);
        }
        System.out.println("Profile fetched: " + (user.value == null ? "FAILED" : "SUCCESS"));
        return user;
    };

    /**
     * Builds a task to fetch page {@code pageNum} of a user's tweets, relying on
     * {@link #twitter} and {@link #screenName}.
     */
    private Callable<List<Wrapper<Status>>> collectTweetsTask(final int pageNum) {
        return  () -> {
            List<Status> returnedTweets = twitter.getUserTimeline(screenName, new Paging(pageNum, TWEET_BATCH_SIZE));

            return returnedTweets.stream()
                                 .map(tweet -> new Wrapper<>(tweet, TwitterObjectFactory.getRawJSON(tweet)))
                                 .collect(Collectors.toList());
        };
    }

    /**
     * A task to fetch page {@code pageNum} of a user's favourite tweets, relying on
     * {@link #twitter} and {@link #screenName}.
     */
    private Callable<List<Wrapper<Status>>> collectFavesTask(final int pageNum) {
        return  () -> {
            List<Status> returnedTweets = twitter.getFavorites(screenName, new Paging(pageNum, TWEET_BATCH_SIZE));

            return returnedTweets.stream()
                                 .map(tweet -> new Wrapper<>(tweet, TwitterObjectFactory.getRawJSON(tweet)))
                                 .collect(Collectors.toList());
        };
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

        try {
            theApp.run();
        } finally {
            theApp.shutdownExecutorSafely();
        }
    }

    public GetUser() {
        this.httpClient = HttpClientBuilder.create().disableRedirectHandling().build();
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

        twitter = new TwitterFactory(buildTwitterConfiguration()).getInstance();

        String name = screenName == null ? "user " + userID : "@" + screenName;
        System.out.println("Retrieving profile and timeline for " + name + ".");

        try {
            // Determine the user's screen name
            Wrapper<User> user = fetchProfile().get();

            // Use that to set up the output directories
            String userDir = rootOutputDir + "/" + screenName;
            new File(userDir).mkdirs();

            // Write out the user's profile
            saveJSON(user.rawJSON, userDir + "/profile.json");

            if (user.value.isProtected()) {
                System.out.println("User @" + screenName + " is protected.");
                return;
            }

            Map<Long, Set<String>> mediaToFetch = new TreeMap<>();
            Map<Long, Set<String>> urlsMentioned = new TreeMap<>();

            // Retrieve and store status updates, as many as possible, one per file
            int tweetCount = fetchTweets(userDir + "/tweets", urlsMentioned, mediaToFetch);

            // Retrieve favourites
            int faveCount = fetchFavourites(userDir + "/favourites", urlsMentioned, mediaToFetch);

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

        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to get timeline: " + te.getMessage());
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            System.err.println("Scheduled call was cancelled somehow");
            e.printStackTrace();
        }
    }

    /**
     * Shuts down the {@link #executor} nicely, giving it an opportunity to clean up after itself.
     */
    private void shutdownExecutorSafely() {
        try {
            System.out.println("attempt to shutdown executor");
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            System.err.println("tasks interrupted");
        }
        finally {
            if (!executor.isTerminated()) {
                System.err.println("cancel non-finished tasks");
            }
            executor.shutdownNow();
            System.out.println("shutdown finished");
        }
    }

    /**
     * Removes any entries from the {@code urlsMentioned} map where the value associated
     * with a key is an empty set.
     *
     * @param urlsMentioned The map of Tweet IDs to sets of URLs mentioned in the Tweet
     */
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

        int[] fetched = { 0 };
        int[] tweetCount = { 0 };
        mediaUrls.entrySet().parallelStream().forEach(mediaUrl -> {
//        for (Map.Entry<Long, Set<String>> mediaUrl : mediaUrls.entrySet()) {
            Long id = mediaUrl.getKey();
            Set<String> urls = mediaUrl.getValue();
            tweetCount[0]++;
            int mediaCount = 1;
            for (String urlStr : urls) {
                urlStr = expandSafe(tweak(urlStr));
                System.out.printf("GET MEDIA %d/%d FROM %s ...", tweetCount[0], mediaUrls.size(), urlStr);
                String ext = urlStr.substring(urlStr.lastIndexOf('.') + 1);
                String filename = id + "-" + (mediaCount++);
                try {
                    URL url = new URL(urlStr);
                    BufferedImage bi = ImageIO.read(url);
                    ImageIO.write(bi, ext, new File(mediaDir + "/" + filename + "." + ext));
                    fetched[0]++;
                    System.out.println(" SUCCESS");
                } catch (IllegalArgumentException | IOException e) {
                    System.out.println(" FAIL(" + e.getMessage() + ") - Skipping");
                }
            }
        });
        return fetched[0];
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
     * Attempts to treat {@code urlArg} as a shortened URL and expand it to as many
     * levels as it goes.
     * <p>
     * Many thanks to <a href="http://baeldung.com/unshorten-url-httpclient">http://baeldung.com/unshorten-url-httpclient</a>.
     *
     * @param urlArg
     *            The potentially shorted URL
     * @return The expanded form of the urlArg, or itself if it's not shortened
     */
    private String expandSafe(String urlArg) {
        System.out.printf("Expanding %s\n", urlArg);
        String originalUrl = urlArg;
        String newUrl = null;
        try {
            newUrl = expandSingleLevelSafe(originalUrl).getRight();
            List<String> alreadyVisited = Lists.newArrayList(originalUrl, newUrl);
            while (!originalUrl.equals(newUrl)) {
                originalUrl = newUrl;
                Pair<Integer, String> statusAndUrl = expandSingleLevelSafe(originalUrl);
                newUrl = statusAndUrl.getRight();
                if (isARedirect(statusAndUrl.getLeft()) && alreadyVisited.contains(newUrl)) {
                    System.err.printf("Likely a redirect loop: %s\n", urlArg);
                    return urlArg;
                }
                alreadyVisited.add(newUrl);
            }
        } catch (IOException ioe) {
            System.err.println("Problem resolving URLs: " + ioe.getMessage());
            ioe.printStackTrace();
            return urlArg;
        }
        return newUrl;
    }

    /**
     * Expands a URL once if it is shortened.
     * <p>
     * Many thanks to <a href="http://baeldung.com/unshorten-url-httpclient">http://baeldung.com/unshorten-url-httpclient</a>.
     *
     * @param url
     *            The URL to expand
     * @return The expanded URL or the URL itself
     * @throws IOException
     *            If an error occurs attempting to retrieve the URL
     */
    private Pair<Integer, String> expandSingleLevelSafe(String url) throws IOException {
        HttpHead request = null;
        try {
            request = new HttpHead(url);
            HttpResponse httpResponse = httpClient.execute(request);

            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if (!isARedirect(statusCode)) {
                return Pair.of(statusCode, url);
            }
            Header[] headers = httpResponse.getHeaders(HttpHeaders.LOCATION);
            Preconditions.checkState(headers.length == 1);
            String newUrl = headers[0].getValue();

            System.out.printf("HTTP response %d: %s -> %s\n", statusCode, url, newUrl);
            return Pair.of(statusCode, newUrl);
        } catch (IllegalStateException | IllegalArgumentException e) {
            // missing LOCATION header or bogus character in the URL
            return Pair.of(-1, url);
        } finally {
            if (request != null) {
                request.releaseConnection();
            }
        }
    }

    /**
     * Checks the HTTP response status {@code code} to see if it's a 301 (moved permanently),
     * a 302 (moved temporarily) or a 307 (temporary redirect).
     *
     * @param code The status code of an HTTP response
     * @return True if the code indicates that the response is a redirect.
     */
    protected static boolean isARedirect(int code) {
        return code == MOVED_PERMANENTLY ||
               code == MOVED_TEMPORARILY ||
               code == TEMPORARY_REDIRECT;
    }

    /**
     * Fetch the tweets of the user and save them to the {@code tweetsDir}. Also records URLs
     * mentioned, and specifically ones that refer to media.
     *
     * @param tweetsDir     the directory to which to save tweets
     * @param urlsMentioned a map of tweet ID to a set of URLs mentioned to update
     * @param mediaToFetch  a map of tweet ID to a set of URLs to media mentioned to update
     * @return the number of tweets successfully retrieved and saved
     * @throws TwitterException if an error occurs talking to the Twitter API
     * @throws IOException if an error occurs talking to the network or the file system
     */
    private int fetchTweets(String tweetsDir, Map<Long, Set<String>> urlsMentioned,
                            Map<Long, Set<String>> mediaToFetch)
        throws TwitterException, IOException, ExecutionException, InterruptedException {
        new File(tweetsDir).mkdirs();

        int pageNum = 1; // reset the page number
        int tweetCount = 0;
        List<Wrapper<Status>> tweets = schedule("/statuses/user_timeline", collectTweetsTask(pageNum)).get();

        while (tweets.size() > 0) {
            processTweets(tweets, tweetsDir, urlsMentioned, mediaToFetch, includeMedia);
            tweetCount += tweets.size();
            System.out.println(tweetCount + " tweets retrieved...");
            pageNum++;
            tweets = schedule("/statuses/user_timeline", collectTweetsTask(pageNum)).get();
        }
        return tweetCount;
    }

    /**
     * Fetch the favourite tweets of the user and save them to the {@code favesDir}. Also
     * records URLs mentioned, and specifically ones that refer to media.
     *
     * @param favesDir      the directory to which to save favourited tweets
     * @param urlsMentioned a map of tweet ID to a set of URLs mentioned to update
     * @param mediaToFetch  a map of tweet ID to a set of URLs to media mentioned to update
     * @return the number of tweets successfully retrieved and saved
     * @throws TwitterException if an error occurs talking to the Twitter API
     * @throws IOException if an error occurs talking to the network or the file system
     */
    private int fetchFavourites(String favesDir, Map<Long, Set<String>> urlsMentioned,
                                Map<Long, Set<String>> mediaToFetch)
        throws TwitterException, IOException, ExecutionException, InterruptedException {
        new File(favesDir).mkdirs();

        int pageNum = 1;
        int faveCount = 0;
        List<Wrapper<Status>> faves = schedule("/favorites/list", collectFavesTask(pageNum)).get();

        while (faves.size() > 0) {
            processTweets(faves, favesDir, urlsMentioned, mediaToFetch, includeFaveMedia);
            faveCount += faves.size();
            System.out.println(faveCount + " favourite tweets retrieved...");
            pageNum++;
            faves = schedule("/favorites/list", collectFavesTask(pageNum)).get();
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
    private void processTweets(final List<Wrapper<Status>> tweets, final String tweetsDir,
                               final Map<Long, Set<String>> urlsMentioned,
                               final Map<Long, Set<String>> mediaToFetch, final boolean inclMediaURLs)
        throws IOException {
        new File(tweetsDir).mkdirs(); // make sure

        for (Wrapper<Status> tweet : tweets) {
            String fileName = tweetsDir + "/" + tweet.value.getId() + ".json";
            saveJSON(tweet.rawJSON, fileName);

            associateURLsWithTweet(tweet.value, collectMentionedURLs(tweet.value), urlsMentioned);
            if (inclMediaURLs) {
                collectMediaURLs(tweet.value, mediaToFetch);
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
     * @return the Twitter4j {@link User profile} of the specified user
     * @throws TwitterException if an error occurs talking to Twitter
     */
    private Future<Wrapper<User>> fetchProfile()
        throws TwitterException, ExecutionException, InterruptedException {
        return schedule(API_FETCH_PROFILE, fetchProfileTask);
    }

    /**
     * Schedules the given {@code apiCall} on the {@link #executor}. A test prior to
     * scheduling is done to check the rate limit status of the API call, as defined
     * by {@code api}.
     *
     * @param api     The Twitter API being called
     * @param apiCall The task calling the API and marshalling the return value
     * @param <T>     The type of the value returned by the API call
     * @return        The value returned from the Twitter API call
     * @throws TwitterException     if there is an error talking to Twitter's API
     * @throws ExecutionException   if there's an error scheduling the task
     * @throws InterruptedException if the executor is interrupted
     */
    private <T> ScheduledFuture<T> schedule(final String api, final Callable<T> apiCall)
        throws TwitterException, ExecutionException, InterruptedException {

        // First check how close we are to our rate limit for this particular call.
        // Because this check is also rate limited, we defer its call to occur after the previous task
        Callable<Long> rateLimitCheck = () -> {
            String resource = api.substring(1, api.indexOf('/', 1)); // "/users/show/:id" -> "users"
            Map<String, RateLimitStatus> limits = twitter.getRateLimitStatus(resource); // just get the info we need
            return getTimeToDefer(limits.get(api));
        };

        if (debug)
            System.out.printf("Checking rate limit for %s in %d seconds from %s\n", api, delay, new Date());

        delay = executor.schedule(rateLimitCheck, delay, TimeUnit.SECONDS).get();

        System.out.println("Scheduling: " + api);
        if (delay != 0) {
            System.out.printf("Delaying by %d seconds, starting at %s\n", delay, new Date());
            delay += 5;
        }

        // If the api call has a delay, then once it's executed there should be no more delay
        // and if there is our rate limit check will be delayed needlessly.
        Callable<T> delayResetter = () -> {
            T result = apiCall.call();
            delay = 0;
            return result;
        };

        return executor.schedule(delayResetter, delay, TimeUnit.SECONDS);
    }

    /**
     * Based on the provided {@link RateLimitStatus}, this determines how long to defer the
     * next API call task.
     *
     * @param rateInfo the most recently received RateLimitStatus
     * @return The time in seconds to delay the next scheduled task
     */
    protected long getTimeToDefer(RateLimitStatus rateInfo) {
        if (debug)
            System.out.println("  calls remaining " + rateInfo.getRemaining());

        return rateInfo.getRemaining() <= 0 // a little breathing room
               ? rateInfo.getSecondsUntilReset()
               : 0;
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
     * Loads the proxy properties from a file {@code "./proxy.properties"}.
     * If {@code "http.proxyPassword"} is not provided, it is asked for on
     * {@code stdin}, and can be typed in at runtime. The properties are set
     * into the System properties as well as returned for inclusion into the
     * Twitter configuration.
     *
     * @return A Properties instance with the supplied proxy properties
     */
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
