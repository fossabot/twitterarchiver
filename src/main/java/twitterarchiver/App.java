package twitterarchiver;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import twitter4j.*;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.PropertyConfiguration;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

/**
 * Hello world!
 */
public class App {

  private static final String TEXT = "t";
  private static final String ID = "i";
  private static final String USER_ID = "u";
  private static final String CREATED_AT = "c";
  private static final String IN_REPLY_TO_ID = "s";
  private static final String RETWEETED_ID = "r";
  private static final String USER_MENTION_IDS = "m";
  private static final String HASHTAGS = "h";
  private static final String URLS = "l";
  private static final String MEDIA = "p";
  private static final String GEO = "g";
  private static final String VERIFIED = "v";
  private static final String FOLLOWERS_FRIENDS_FAVS_STATUSES_LISTED = "z";
  private static final String LANG = "n";

  private static class StreamProvider {

    private final String type;

    public StreamProvider(String type) {
      this.type = type;
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          synchronized (StreamProvider.this) {
            try {
              stream.close();
              running = false;
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      });
    }

    private boolean running = true;
    private long last = 0;
    private OutputStream stream;

    public synchronized OutputStream getStream() throws IOException {
      if (running) {
        long newlast = (System.currentTimeMillis() / 3600000) * 3600000l; // hour markers
        if (newlast > last || stream == null) {
          if (stream != null) {
            stream.close();
          }
          last = newlast;
          // Ensures that it doesn't write over a previous file if you stop and restart
          stream = new GZIPOutputStream(new FileOutputStream("sample" + System.currentTimeMillis() + "." + type + ".gz"));
        }
        return stream;
      } else {
        return null;
      }
    }
  }

  public static void main(String[] args) throws IOException {
    TwitterStreamFactory tsf = new TwitterStreamFactory();
    Authorization auth = new OAuthAuthorization(new PropertyConfiguration(App.class.getResourceAsStream("/auth.properties")));
    TwitterStream ts = tsf.getInstance(auth);
    final StreamProvider jsonStreamProvider = new StreamProvider("json");
    final RingBuffer ring = new RingBuffer(100);
    final AtomicLong last = new AtomicLong(0);
    ts.addListener(new StatusAdapter() {
      JsonFactory jf = new MappingJsonFactory();

      @Override
      public void onStatus(Status s) {
        try {
          OutputStream jsonStream = jsonStreamProvider.getStream();
          if (jsonStream != null) {
            writeJson(s, jsonStream);
            if (System.currentTimeMillis() / 1000l > last.longValue()) {
              last.set(System.currentTimeMillis() / 1000l);
              System.out.println(ring.average());
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      private void writeJson(Status s, OutputStream stream) throws IOException {
        long time = s.getCreatedAt().getTime();
        ring.append((int) (System.currentTimeMillis() - time));
        JsonGenerator g = jf.createGenerator(stream);
        g.writeStartObject();
        g.writeStringField(TEXT, s.getText());
        g.writeNumberField(ID, s.getId());
        g.writeNumberField(USER_ID, s.getUser().getId());
        g.writeNumberField(CREATED_AT, time);
        long inReplyToStatusId = s.getInReplyToStatusId();
        if (inReplyToStatusId != -1) {
          g.writeNumberField(IN_REPLY_TO_ID, inReplyToStatusId);
        }
        Status retweetedStatus = s.getRetweetedStatus();
        if (retweetedStatus != null) {
          g.writeNumberField(RETWEETED_ID, retweetedStatus.getId());
        }
        UserMentionEntity[] userMentionEntities = s.getUserMentionEntities();
        if (userMentionEntities != null && userMentionEntities.length > 0) {
          g.writeArrayFieldStart(USER_MENTION_IDS);
          for (UserMentionEntity userMentionEntity : userMentionEntities) {
            g.writeNumber(userMentionEntity.getId());
          }
          g.writeEndArray();
        }
        HashtagEntity[] hashtagEntities = s.getHashtagEntities();
        if (hashtagEntities != null && hashtagEntities.length > 0) {
          g.writeArrayFieldStart(HASHTAGS);
          for (HashtagEntity hashtagEntity : hashtagEntities) {
            g.writeString(hashtagEntity.getText());
          }
          g.writeEndArray();
        }
        URLEntity[] urlEntities = s.getURLEntities();
        if (urlEntities != null && urlEntities.length > 0) {
          g.writeArrayFieldStart(URLS);
          for (URLEntity urlEntity : urlEntities) {
            URL expandedURL = urlEntity.getExpandedURL();
            if (expandedURL == null) {
              g.writeString(urlEntity.getURL().toString());
            } else {
              g.writeString(expandedURL.toString());
            }
          }
          g.writeEndArray();
        }
        MediaEntity[] mediaEntities = s.getMediaEntities();
        if (mediaEntities != null && mediaEntities.length > 0) {
          g.writeArrayFieldStart(MEDIA);
          for (MediaEntity mediaEntity : mediaEntities) {
            g.writeString(mediaEntity.getMediaURL().toString());
          }
          g.writeEndArray();
        }
        GeoLocation geoLocation = s.getGeoLocation();
        if (geoLocation != null) {
          g.writeArrayFieldStart(GEO);
          g.writeNumber(geoLocation.getLatitude());
          g.writeNumber(geoLocation.getLongitude());
          g.writeEndArray();
        }
        if (s.getUser().isVerified()) {
          g.writeBooleanField(VERIFIED, true);
        }
        g.writeArrayFieldStart(FOLLOWERS_FRIENDS_FAVS_STATUSES_LISTED);
        g.writeNumber(s.getUser().getFollowersCount());
        g.writeNumber(s.getUser().getFriendsCount());
        g.writeNumber(s.getUser().getFavouritesCount());
        g.writeNumber(s.getUser().getStatusesCount());
        g.writeNumber(s.getUser().getListedCount());
        g.writeEndArray();
        g.writeStringField(LANG, s.getUser().getLang());
        g.writeEndObject();
        synchronized (this) {
          g.flush();
          stream.write('\n');
        }
      }
    });
    ts.sample();
  }
}
