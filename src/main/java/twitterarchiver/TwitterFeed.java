package twitterarchiver;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * This class is designed to evenly read the feed from Twitter.
 * <p/>
 * User: sam
 * Date: Feb 14, 2010
 * Time: 6:37:48 PM
 */
public class TwitterFeed implements Runnable {

  private static Logger log = Logger.getLogger(TwitterFeed.class.getName());
  private final Set<TwitterFeedListener> sls = new HashSet<TwitterFeedListener>();
  private final Map<TwitterFeedListener, AtomicInteger> counts = new HashMap<TwitterFeedListener, AtomicInteger>();
  private final ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  private final JsonFactory jf = new MappingJsonFactory();

  private String username;
  private String password;
  private String url;
  private int maxWaitTime = 5000;
  private int total;

  public TwitterFeed(String url) {
    this.url = url;
  }

  public TwitterFeed(String username, String password, String url, int maxWaitTime) {
    this(url);
    this.username = username;
    this.password = password;
    this.maxWaitTime = maxWaitTime;
  }

  public TwitterFeed(String username, String password, String url, int maxWaitTime, int total) {
    this(username, password, url, maxWaitTime);
    this.total = total;
  }

  public void addEventListener(TwitterFeedListener sl) {
    synchronized (sls) {
      sls.add(sl);
      counts.put(sl, new AtomicInteger(0));
    }
  }

  public void removeEventListener(TwitterFeedListener sl) {
    synchronized (sls) {
      sls.remove(sl);
      counts.remove(sl);
    }
  }

  public void run() {
    try {
      int retries = 0;
      int waitTime = maxWaitTime;
      final AtomicInteger total = new AtomicInteger(0);
      boolean complete = false;
      do {
        DefaultHttpClient hc = new DefaultHttpClient();
        if (username != null) {
          BasicCredentialsProvider provider = new BasicCredentialsProvider();
          provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
          hc.setCredentialsProvider(provider);
        }
        final HttpGet stream = new HttpGet(url);
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
          int current = 0;

          @Override
          public void run() {
            if (total.intValue() == current) {
              log.warning("Aborted stream. No data for " + maxWaitTime / 1000 + " seconds.");
              stream.abort();
              this.cancel();
            } else {
              current = total.intValue();
            }
          }
        }, waitTime, waitTime);
        try {
          final HttpResponse response = hc.execute(stream);
          BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
          String line;
          log.info("Reading stream");
          while (!complete && (line = br.readLine()) != null) {
            if (!line.equals("")) {
              updateListeners(line);
              total.incrementAndGet();
              retries = 0;
            }
            if (TwitterFeed.this.total != 0) {
              complete = total.get() >= TwitterFeed.this.total;
            }
          }
          log.info("Done reading stream");
          br.close();
        } catch (Exception e) {
          log.severe(e.getMessage());
          if (retries != 0) {
            Thread.sleep(waitTime);
            waitTime = Math.min(waitTime * 2, 240000);
          }
          retries++;
        }
      } while (retries < 10 && !complete);
      if (complete) {
        log.info("Completed successfully");
      } else {
        log.info("Failed after 10 retries");
      }
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  private void updateListeners(final String line) {
    synchronized (sls) {
      es.submit(new Runnable() {
        @Override
        public void run() {
          try {
            JsonParser parser = jf.createJsonParser(line);
            final JsonNode node = parser.readValueAsTree();
            for (final TwitterFeedListener sl : sls) {
              final AtomicInteger count = counts.get(sl);
              if (count.intValue() > 10000) {
                sl.tooSlow();
              } else {
                count.incrementAndGet();
                es.submit(new Runnable() {
                  public void run() {
                    try {
                      sl.messageReceived(new TwitterFeedEvent(node, line));
                    } finally {
                      count.decrementAndGet();
                    }
                  }
                });
              }
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
  }
}