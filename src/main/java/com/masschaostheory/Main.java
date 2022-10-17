package com.masschaostheory;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.exceptions.MqttClientStateException;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Main {

    private static final String brokerHost = "broker.hivemq.com";

    public static void main(String[] args) throws InterruptedException {
        Scanner in = new Scanner(System.in);
        System.out.println("### MQTT Messenger Bot, at your service! ###");

        System.out.println("How many MQTT v3 Workers should I use? (0 - 255)");
        int mqtt3ThreadCount = in.nextInt();

        System.out.println("How many MQTT v5 Workers should I use? (0 - 255)");
        int mqtt5ThreadCount = in.nextInt();

        System.out.println("Summary:\n-->MQTT v3 Workers: " + mqtt3ThreadCount + "\n-->MQTT v5 Workers: " + mqtt5ThreadCount);

        System.out.println();
        System.out.println( "------------------------------" );

        System.out.println("Generating MQTT Workers...");
        int totalThreadCount = mqtt3ThreadCount + mqtt5ThreadCount;
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(totalThreadCount);
        for ( int i = 0; i < totalThreadCount; i++ )
        {
            if ( i < mqtt3ThreadCount )
                executor.submit( new Mqtt3Worker( i ) );
            else
                executor.submit( new Mqtt5Worker( i ) );
        }

        executor.shutdown();
    }

    private static class Mqtt3Worker implements MqttWorker
    {
        private final int ID;

        private final String MSG;

        private final String UID = UUID.randomUUID().toString();

        /**
         * A MQTT v3 blocking client
         */
        private final Mqtt3BlockingClient client = Mqtt3Client.builder()
                .identifier( UID )
                .serverHost( brokerHost )
                .buildBlocking();

        Mqtt3Worker( int workerId )
        {
            this.ID = workerId;
            this.MSG = "Test message from worker" + workerId;
        }

        @Override
        public void run()
        {
            try
            {
                System.out.println( "Mqtt v3 Worker " + ID + " starting..." );
                client.connect();
                logConnect( ID, UID );

                Thread.sleep( SHORT_SLEEP );

                if ( !client.getState().isConnected() )
                {
                    System.out.println( "Client " + UID + " disconnected unexpectedly!" );
                    return;
                }

                for ( int i = 0; i < 10; i++ ) {
                    client.publishWith()
                            .topic( "test/topic" )
                            .qos( MqttQos.AT_LEAST_ONCE )
                            .payload( MSG.getBytes() )
                            .send();
                    logPublish( ID );

                    Thread.sleep( LONG_SLEEP );
                }

                client.disconnect();
                logDisconnect( ID, UID );

                System.out.println( "Worker " + ID + " finished!");
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private static class Mqtt5Worker implements MqttWorker
    {
        private final int ID;

        private final String MSG;

        private final String UID = UUID.randomUUID().toString();

        /**
         * A MQTT v3 blocking client
         */
        private final Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier( UID )
                .serverHost( brokerHost )
                .buildBlocking();

        Mqtt5Worker( int workerId )
        {
            this.ID = workerId;
            this.MSG = "Test message from worker" + workerId;
        }

        @Override
        public void run()
        {
            try
            {
                System.out.println( "Mqtt v5 Worker " + ID + " starting..." );
                client.connect();
                logConnect( ID, UID );

                Thread.sleep( SHORT_SLEEP );

                if ( !client.getState().isConnected() )
                {
                    System.out.println( "Client " + UID + " disconnected unexpectedly!" );
                    return;
                }

                for ( int i = 0; i < 10; i++ ) {
                    client.publishWith()
                            .topic( "test/topic" )
                            .qos( MqttQos.AT_LEAST_ONCE )
                            .payload( MSG.getBytes() )
                            .send();
                    logPublish( ID );

                    Thread.sleep( LONG_SLEEP );
                }

                client.disconnect();
                logDisconnect( ID, UID );

                System.out.println( "Worker " + ID + " finished!");
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}