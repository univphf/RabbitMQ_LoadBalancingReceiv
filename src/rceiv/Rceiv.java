package rceiv;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author tondeur-h
 */
public class Rceiv {

        final private String QUEUE_NAME="GAMADM";
    String message=null;

    public Rceiv() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");

            Connection conn=factory.newConnection();

            Channel channel=conn.createChannel();

            channel.basicQos(1);
               channel.queueDeclare(QUEUE_NAME, false, false, false, null);

          Consumer consumer=new DefaultConsumer(channel)
          {
              @Override
              public void handleDelivery(String consumerTag, Envelope envelop,AMQP.BasicProperties properties, byte[] body ) throws UnsupportedEncodingException, IOException
              {
                  System.out.println("----------------------------");
                  System.out.println("consumerTag="+consumerTag);
                  System.out.println("Taille du Payload="+body.length);
                  System.out.println("DeliveryTag="+envelop.getDeliveryTag());
                  System.out.println("Exchange Name="+envelop.getExchange());
                  System.out.println("Routing Key="+envelop.getRoutingKey());
                  System.out.println("redelivrable="+envelop.isRedeliver());
                  System.out.println("App Id: "+properties.getAppId());

                  message=new String(body,"UTF-8");
                  System.out.println("Recu : "+message);
                  doJob();
                  channel.basicAck(envelop.getDeliveryTag(), false);
                  System.out.println(message +" traité");
              }

                private void doJob() {
                  try {
                      Thread.sleep(1000);
                  } catch (InterruptedException ex) {
                      Logger.getLogger(Rceiv.class.getName()).log(Level.SEVERE, null, ex);
                  }

                }
          } ;

          channel.basicConsume(QUEUE_NAME, false, consumer);

            System.out.println("Démarrage du consommateur OK...");

        } catch (IOException | TimeoutException ex) {
            Logger.getLogger(Rceiv.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        new Rceiv();
    }

}
