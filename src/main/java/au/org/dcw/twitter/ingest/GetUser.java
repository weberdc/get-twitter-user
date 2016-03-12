package au.org.dcw.twitter.ingest;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;


/**
 * Application that gets public timeline and profile for a Twitter user based
 * on their screen name and stores the raw JSON responses into the
 * {@link #rootOutputDir} directory, with the same name as the {@link #screenName}.<p>
 *
 * Borrows heavily from the <a href="http://twitter4j.org">Twitter4j</a> example:
 * @see <a href="https://github.com/yusuke/twitter4j/blob/master/twitter4j-examples/src/main/java/twitter4j/examples/json/SaveRawJSON.java">SaveRawJSON.java</a></a>
 */
public final class GetUser {

    public static final int TWEET_BATCH_SIZE = 200;

    @Parameter(names = { "-s", "--screen-name" }, description = "Twitter screen name")
    private String screenName;

    @Parameter(names = { "-u", "--user-id" }, description = "Twitter user ID (@ symbol optional)")
    private Long userID;

    @Parameter(names = { "-o", "--output" }, description = "Directory to which to write output")
    private String rootOutputDir = "./output";

    @Parameter(names = { "-c", "--credentials"},
               description = "Properties file with Twitter OAuth credentials")
    private String credentialsFile = "./twitter.properties";

    @Parameter(names = "-debug", description = "Debug mode")
    private boolean debug = false;

    /**
     * Usage: java au.org.dcw.twitter.ingest.GetUser [options]
     *   Options:
     *     -c, --credentials
     *        Properties file with Twitter OAuth credentials
     *        Default: ./twitter.properties
     *     -o, --output
     *        Directory to which to write output
     *        Default: ./output
     *     -s, --screen-name
     *        Twitter screen name
     *     -u, --user-id
     *        Twitter user ID
     *     -debug
     *        Debug mode
     *        Default: false
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) throws IOException {
        GetUser theApp = new GetUser();

        JCommander argsParser = new JCommander(theApp, args);

        if (! checkFieldsOf(theApp)) {
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
        if (screenName != null && screenName.startsWith("@"))
            screenName =  screenName.substring(1);

        System.out.println("== Configuration ==");
        System.out.println("screenName: " + screenName);
        System.out.println("userID: " + userID);
        System.out.println("identity: " + credentialsFile);
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
        String name = screenName == null ? "user " + userID : "@" + screenName;
        System.out.println("Saving profile and timeline for " + name + ".");

        try {
            // Determine the user's screen name
            User user = null;
            if (screenName != null) {
                user = twitter.showUser(screenName);
                userID = Long.valueOf(user.getId());
            } else if (userID != null) {
                user = twitter.showUser(userID.longValue());
                screenName = user.getScreenName();
            }

            // Use that to set up the output directories
            String userDir = rootOutputDir + "/" + screenName;
            String statusesDir = userDir + "/statuses";
            String favesDir = userDir + "/favourites";
            new File(rootOutputDir).mkdirs();
            new File(userDir).mkdirs();
            new File(statusesDir).mkdirs();
            new File(favesDir).mkdirs();

            // Write out the user's profile
            saveJSON(TwitterObjectFactory.getRawJSON(user), userDir + "/profile.json");

            // Retrieve and store status updates, as many as possible, one per file
            int i = 1, tweetCount = 0;
            List<Status> statuses = twitter.getUserTimeline(screenName, new Paging(i++, TWEET_BATCH_SIZE));
            while (statuses.size() > 0) {
                for (Status status : statuses) {
                    String rawJSON = TwitterObjectFactory.getRawJSON(status);
                    String fileName = statusesDir + "/" + status.getId() + ".json";
                    saveJSON(rawJSON, fileName);
                }
                tweetCount += statuses.size();
                System.out.println(tweetCount + " tweets retrieved...");
                statuses = twitter.getUserTimeline(screenName, new Paging(i++, TWEET_BATCH_SIZE));
            }

            // Retrieve favourites
            i = 1;
            int faveCount = 0;
            List<Status> faves = twitter.getFavorites(screenName, new Paging(i++, TWEET_BATCH_SIZE));
            while (faves.size() > 0) {
                for (Status status : faves) {
                    String rawJSON = TwitterObjectFactory.getRawJSON(status);
                    String fileName = favesDir + "/" + status.getId() + ".json";
                    saveJSON(rawJSON, fileName);
                }
                faveCount += faves.size();
                System.out.println(faveCount + " favourite tweets retrieved...");
                faves = twitter.getFavorites(screenName, new Paging(i++, TWEET_BATCH_SIZE));
            }

            // Report findings
            System.out.printf("\nUser @%s (%s)\n", screenName, userID.toString());
            System.out.println(" - tweets: " + tweetCount);
            System.out.println(" - favourites: " + faveCount);
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
     * @param rawJSON the JSON String to persist
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
