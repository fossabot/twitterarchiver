package twitterarchiver;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hello world!
 */
public class App {

  @Argument
  private static String hose = "sample";

  @Argument
  private static Boolean upload = false;

  public static void main(String[] args) throws IOException {
    try {
      Args.parse(App.class, args);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      Args.usage(App.class);
      System.exit(1);
    }
    final AtomicLong tweets = new AtomicLong(0);
    final AtomicLong dropped = new AtomicLong(0);
    final AtomicLong last = new AtomicLong(System.currentTimeMillis());
    Properties auth = new Properties();
    auth.load(App.class.getResourceAsStream("/auth.properties"));
    String host = auth.getProperty("host");
    if (host == null) {
      host = "stream.twitter.com";
    }
    TwitterFeed twitterFeed = new TwitterFeed(auth.getProperty("username"),
            auth.getProperty("password"),
            "https://" + host + "/1/statuses/" + hose + ".json", 600000);// 10 minutes
    final StreamProvider jsonStreamProvider = new StreamProvider(hose);
    // Get the filename we are going to use
    jsonStreamProvider.getStream();
    TwitterFeedUploader uploader = new TwitterFeedUploader(hose, ".json.gz", jsonStreamProvider);
    uploader.start();
    if (!upload) {
      twitterFeed.addEventListener(new TweetSerializer(jsonStreamProvider, dropped, tweets, last));
      twitterFeed.addEventListener(new UserStorer());
      twitterFeed.run();
    }
  }

}
