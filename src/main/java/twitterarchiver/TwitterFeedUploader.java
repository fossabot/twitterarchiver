package twitterarchiver;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Long.parseLong;
import static java.util.regex.Pattern.quote;

/**
 * Look in the current directory for files to upload that are not the current file and upload them to S3.
 */
public class TwitterFeedUploader extends TimerTask {
  private final String prefix;
  private final String suffix;
  private final StreamProvider streamProvider;
  private final Timer timer;
  private final AmazonS3Client client;

  public TwitterFeedUploader(String prefix, String suffix, StreamProvider streamProvider) {
    this.prefix = prefix;
    this.suffix = suffix;
    this.streamProvider = streamProvider;
    timer = new Timer("TwitterFeedUploader", true);
    try {
      Properties awsCredentials = new Properties();
      awsCredentials.load(TwitterFeedUploader.class.getResourceAsStream("/aws.properties"));
      AWSCredentials credentials = new BasicAWSCredentials(awsCredentials.getProperty("accessKey"), awsCredentials.getProperty("secretKey"));
      client = new AmazonS3Client(credentials);
    } catch (IOException e) {
      throw new AssertionError("No credentials found");
    }
  }

  public void start() {
    timer.scheduleAtFixedRate(this, 0, 60 * 60 * 1000l /* 60 minutes */);
  }

  private Semaphore semaphore = new Semaphore(1);

  @Override
  public void run() {
    if (semaphore.tryAcquire()) {
      try {
        File currentDir = new File(".");
        final Pattern GET_TIMESTAMP = Pattern.compile(quote(prefix) + "([0-9]+)" + quote(suffix));
        String[] list = currentDir.list(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            String filename = streamProvider.getFilename();
            return GET_TIMESTAMP.matcher(name).matches() && !name.equals(filename);
          }
        });
        for (String s : list) {
          Matcher matcher = GET_TIMESTAMP.matcher(s);
          if (matcher.matches()) {
            long timestamp = parseLong(matcher.group(1));
            Calendar cal = GregorianCalendar.getInstance();
            cal.setTimeInMillis(timestamp);
            // Construct filename
            File prefixDir = new File(prefix);
            File yearDir = new File(prefixDir, String.valueOf(cal.get(Calendar.YEAR)));
            File monthDir = new File(yearDir, String.valueOf(cal.get(Calendar.MONTH) + 1));
            File dayDir = new File(monthDir, String.valueOf(cal.get(Calendar.DAY_OF_MONTH)));
            File hourDir = new File(dayDir, String.valueOf(cal.get(Calendar.HOUR_OF_DAY)));
            File s3FileName = new File(hourDir, s);
            File localFile = new File(s);
            long start = System.currentTimeMillis();
            System.out.print("Uploading " + localFile + " to " + s3FileName + "...");
            PutObjectRequest por = new PutObjectRequest("com.sampullara.twitterfeed", s3FileName.toString(), localFile);
            por.setStorageClass(StorageClass.ReducedRedundancy);
            client.putObject(por);
            long diff = System.currentTimeMillis() - start;
            System.out.println("in " + diff + "ms.");
            localFile.delete();
          }
        }
      } finally {
        semaphore.release();
      }
    }
  }
}