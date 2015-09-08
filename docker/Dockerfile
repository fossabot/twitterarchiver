FROM spullara/java8:8u25
ADD auth.properties auth.properties
ADD twitterarchiver.jar twitterarchiver.jar
CMD ["java", "-cp", ".:twitterarchiver.jar", "twitterarchiver.App", "-wavefront", "https://dev.corp.wavefront.com"]


