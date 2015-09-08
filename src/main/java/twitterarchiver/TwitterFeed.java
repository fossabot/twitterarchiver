package twitterarchiver;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DecompressingHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

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
import java.util.concurrent.atomic.AtomicBoolean;
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
  private Counter lines;
  private Counter connections;
  private AtomicBoolean currentlyConnected = new AtomicBoolean();
  private Gauge<Integer> connected;

  public TwitterFeed(String url) {
    this.url = url;
  }

  public TwitterFeed(String username, String password, String url, int maxWaitTime) {
    this(url);
    this.username = username;
    this.password = password;
    this.maxWaitTime = maxWaitTime;
    lines = Metrics.newCounter(TwitterFeed.class, "lines");
    connections = Metrics.newCounter(TwitterFeed.class, "connections");
    connected = Metrics.newGauge(TwitterFeed.class, "connected", new Gauge<Integer>() {
      @Override
      public Integer value() {
        return currentlyConnected.get() ? 1 : 0;
      }
    });
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
        // Timeout after 10s
        HttpParams params = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(params, 10000);
        HttpConnectionParams.setSoTimeout(params, 10000);
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
          DecompressingHttpClient dhc = new DecompressingHttpClient(hc);
          final HttpResponse response = dhc.execute(stream);
          BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
          String line;
          log.info("Reading stream");
          connections.inc();
          currentlyConnected.set(true);
          try {
            while (!complete && (line = br.readLine()) != null) {
              if (!line.equals("")) {
                lines.inc();
                updateListeners(line);
                total.incrementAndGet();
                retries = 0;
              }
              if (TwitterFeed.this.total != 0) {
                complete = total.get() >= TwitterFeed.this.total;
              }
            }
          } finally {
            currentlyConnected.set(false);
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

  private static final AtomicInteger totalCount = new AtomicInteger(0);

  private void updateListeners(final String line) {
    synchronized (sls) {
      if (totalCount.intValue() > 100000) {
        for (TwitterFeedListener sl : sls) {
          sl.tooSlow();
        }
      } else {
        totalCount.incrementAndGet();
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
              System.err.println("Failed to parse: " + line + ", " + e.getMessage());
            } finally {
              totalCount.decrementAndGet();
            }
          }
        });
      }
    }
  }
}