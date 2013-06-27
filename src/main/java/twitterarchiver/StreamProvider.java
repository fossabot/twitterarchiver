package twitterarchiver;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.GZIPOutputStream;

/**
 * Returns the current filename we should use.
 */
public class StreamProvider {

  private final Timer timer;
  private String prefix;
  private String filename;

  public StreamProvider(String prefix) {
    this.prefix = prefix;
    timer = new Timer();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        OutputStream previousStream = stream;
        synchronized (StreamProvider.this) {
          running = false;
          stream = null;
        }
        try {
          Thread.sleep(1000);
          previousStream.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  private boolean running = true;
  private long last = 0;
  private OutputStream stream;

  public synchronized String getFilename() {
    return filename;
  }

  public OutputStream getStream() throws IOException {
    if (running) {
      long newlast = (System.currentTimeMillis() / 3600000) * 3600000l; // hour markers
      if (newlast > last || stream == null) {
        final OutputStream previousStream = stream;
        synchronized (this) {
          last = newlast;
          // Ensures that it doesn't write over a previous file if you stop and restart
          filename = prefix + System.currentTimeMillis() + ".json.gz";
          stream = new GZIPOutputStream(new FileOutputStream(filename));
        }
        if (previousStream != null) {
          // Need to wait for the system to stop writing to the old stream
          timer.schedule(new TimerTask() {
            @Override
            public void run() {
              try {
                previousStream.close();
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          }, 1000);
        }
      }
      return stream;
    } else {
      return null;
    }
  }
}
