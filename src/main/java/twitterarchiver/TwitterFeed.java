package twitterarchiver;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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
public class TwitterFeed {

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

  public TwitterFeed(String consumerKey, String consumerSecret, String token, String secret) {
    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
    final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(1000);

    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesSampleEndpoint hosebirdEndpoint = new StatusesSampleEndpoint();

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

    ClientBuilder builder = new ClientBuilder()
            .name("Sample-Hose-Client")                              // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue))
            .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

    Client hosebirdClient = builder.build();
    // Attempts to establish a connection.
    hosebirdClient.connect();

    lines = Metrics.newCounter(TwitterFeed.class, "lines");
    es.submit(new Runnable() {
                @Override
                public void run() {
                  try {
                    String line;
                    while ((line = msgQueue.take()) != null) {
                      if (!line.equals("")) {
                        lines.inc();
                        updateListeners(line);
                      }
                    }
                  } catch (Throwable e) {
                    e.printStackTrace();
                  }
                }
              }

    );

    es.submit(new Runnable() {
                @Override
                public void run() {
                  Event event;
                  try {
                    while ((event = eventQueue.take()) != null) {
                      System.out.println(event.getMessage());
                    }
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }
              }

    );
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