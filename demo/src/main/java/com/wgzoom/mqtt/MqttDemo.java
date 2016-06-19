package com.wgzoom.mqtt;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttDemo {
    public static void main(String[] args) throws IOException {
        String broker = null;
        String acessKey = null;
        String secretKey = null;
        String clientId = null;
        Properties properties = new Properties();
        properties.load(MqttDemo.class.getClassLoader().getResourceAsStream("user.properties"));
        broker = properties.getProperty("broker");
        acessKey = properties.getProperty("AccessKey");
        secretKey = properties.getProperty("SecretKey");
        final String topic = properties.getProperty("Topic"); 
        final String[] topicFilters=new String[]{"topic1"};
        clientId = properties.getProperty("ConsumerId");
        //String sign;
        MemoryPersistence persistence = new MemoryPersistence();
        try {
            final MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            final MqttConnectOptions connOpts = new MqttConnectOptions();
            System.out.println("Connecting to broker: " + broker);
            connOpts.setUserName(acessKey);
            connOpts.setServerURIs(new String[] { broker });
            connOpts.setPassword(secretKey.toCharArray());
            connOpts.setCleanSession(false);
            connOpts.setKeepAliveInterval(100);
            sampleClient.setCallback(new MqttCallback() {
                public void connectionLost(Throwable throwable) {
                    while (true) {
                        try {
                            System.out.println("connectionLost");
                            throwable.printStackTrace();
                            Thread.sleep(1000L);
                            sampleClient.connect(connOpts);
                            System.out.println("reconnect success");
                            sampleClient.subscribe(topicFilters);
                            System.out.println("subscribe....success");
                            break;
                        } catch (MqttSecurityException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (MqttException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                            if (e.getReasonCode()==MqttException.REASON_CODE_CLIENT_CONNECTED) {
                                break;
                            }
                        }catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
                    System.out.println("messageArrived:" + topic + "------" + new String(mqttMessage.getPayload()));
                }
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                    System.out.println("deliveryComplete:" + iMqttDeliveryToken.getMessageId());
                }
            });
            sampleClient.connect(connOpts);
            //sampleClient.subscribe(topicFilters);
            System.out.println("subscribe....success");
            Thread.sleep(1000L);
            for (int i = 0; i < 3; i++) {
                try {
                    String scontent = new Date()+" MQTT Test body " + i;
                    final MqttMessage message = new MqttMessage(scontent.getBytes());
                    message.setQos(1);
                    message.setRetained(true);
                    sampleClient.publish(topic+"", message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Thread.sleep(5000);
            sampleClient.disconnect();
            sampleClient.close();
            System.out.println("exit...");
        } catch (Exception me) {
            me.printStackTrace();
        }
    }
}
