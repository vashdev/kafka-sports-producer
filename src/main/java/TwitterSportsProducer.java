import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

// Get sports tweets and hyderate sports topic locally on kafka cluster

public class TwitterSportsProducer {
    private static String HASHTAG="soccer";
    private static String TOPICNAME="sports";

    public static void main(String[] args) {
        System.out.println(" Twitter api as source");
        new TwitterSportsProducer().run();


    }
   public void  run()  {
       // create twitter client
       BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
       Client client = createTwitterCleints(msgQueue);
       // attempts to establish connection
       client.connect();

       // kafka producer
       KafkaProducer<String,String> producer =  createKafkaProducer();

       // loop to send tweets to kafka
       // on a different thread, or multiple different threads....
       while (!client.isDone()) {
           String msg = null;
           try {
               msg = msgQueue.poll(5, TimeUnit.MINUTES);
           } catch (InterruptedException e) {
               e.printStackTrace();
               client.stop();
           }
           if (msg !=null ) {
               System.out.println(msg);
               producer.send(new ProducerRecord<String, String>( TOPICNAME, msg), new Callback() {
                   @Override
                   public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                       if ( e!=null){
                           System.out.println(" Error  processing records");
                       }
                   }
               });
           }
          System.out.println(" App End ...");
       }

   }

   public Client createTwitterCleints(BlockingQueue<String> msgQueue){

      // BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
       /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
       Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
       StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
       //people
       //List<Long> followings = Lists.newArrayList(1234L, 566788L);
       // terms
       List<String> terms = Lists.newArrayList(HASHTAG);
      // hosebirdEndpoint.followings(followings);
       hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
       String consumerKey="TTkZ5zRYiotJr2VjkPnjJ2CNa";
       String consumerSecret="SAXjZl9dOp6ylmkAhBybiQXtLE7PeUCpGqhCQnziLoeRVL065s";
       String accessToken= "1960161991-3oJ2SxVMk8y18Zw2x9npzMJTORuSQfLFDwU7ZLJ";
       String accessTokenSecret="dnn6ysCH0uheAbzFKjpdPYH1Ux3oUZdK6vNq9G6I6fUg6";
       Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);

       ClientBuilder builder = new ClientBuilder()
               .name("Hosebird-Client-01")                              // optional: mainly for the logs
               .hosts(hosebirdHosts)
               .authentication(hosebirdAuth)
               .endpoint(hosebirdEndpoint)
               .processor(new StringDelimitedProcessor(msgQueue));
                                       // optional: use this if you want to process client events

       Client hosebirdClient = builder.build();
// Attempts to establish a connection.
       return hosebirdClient;

   }
   public KafkaProducer<String,String> createKafkaProducer(){
       // create producer
       Properties prop = new Properties();
       String bootstrapServers="127.0.0.1:9092";

       prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
       prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
       prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
       // safe producer
       prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
       // not required because its idempotent nayway but for readability
       prop.setProperty(ProducerConfig.ACKS_CONFIG,"all");
       prop.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
       prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

       // High thruput at expence of bit latency
       prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
       prop.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
       prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); //32KB
       KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);


       // Send Data
       // produce recs
       // ProducerRecord<String,String> rec = new ProducerRecord<String, String>("first_topic","Test first static msg production");

       return producer;
   }
}
