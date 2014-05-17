package twitterarchiver;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import com.yammer.metrics.Metrics;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Hello world!
 */
public class App {

  private static Logger log = Logger.getLogger("TwitterFeed");

  @Argument
  private static String hose = "sample";

  @Argument
  private static Boolean upload = false;

  @Argument
  private static Boolean users = false;

  @Argument
  private static String wavefront;

  public static void main(String[] args) throws IOException {
    try {
      Args.parse(App.class, args);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      Args.usage(App.class);
      System.exit(1);
    }
    Properties auth = new Properties();
    auth.load(App.class.getResourceAsStream("/auth.properties"));
    if (wavefront != null) {
      System.out.println("Starting metrics reporter");
      JsonMetricsReporter mr = new JsonMetricsReporter("twitterfeed", Metrics.defaultRegistry(), auth.getProperty("sunnylabstoken"), wavefront);
      mr.start(1, TimeUnit.MINUTES);
    }
    String host = auth.getProperty("host");
    if (host == null) {
      host = "stream.twitter.com";
    }
    String url = "https://" + host + "/1/statuses/" + hose + ".json";
    TwitterFeed twitterFeed = new TwitterFeed(auth.getProperty("username"),
            auth.getProperty("password"),
            url, 600000);// 10 minutes
    log.info("Connecting to: " + url);
    final StreamProvider jsonStreamProvider = new StreamProvider(hose);
    // Get the filename we are going to use
    jsonStreamProvider.getStream();
    TwitterFeedUploader uploader = new TwitterFeedUploader(hose, ".json.gz", jsonStreamProvider);
    uploader.start();
    if (!upload) {
      twitterFeed.addEventListener(new TweetSerializer(jsonStreamProvider));
      if (users) {
        twitterFeed.addEventListener(new UserStorer());
      }
      twitterFeed.run();
    }
  }

}
