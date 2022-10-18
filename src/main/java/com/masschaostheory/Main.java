package com.masschaostheory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.masschaostheory.entity.MqttSensor;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;

public class Main {

    //private static final String brokerHost = "broker.hivemq.com";
    private static final String brokerHost = "192.168.1.50";

    private static float randomTempVal( MqttSensor sensor )
    {
        float max = 52f;
        float min  = 49f;

        if ( sensor.getId().contains("basement") )
        {
            max = 66f;
            min  = 65f;
        }
        else if ( sensor.getId().contains("office") )
        {
            max = 75f;
            min = 73f;
        }
        else if ( sensor.getId().contains("bedroom") )
        {
            max = 72f;
            min = 71f;
        }
        else if ( sensor.getId().contains("livingroom") )
        {
            max = 74f;
            min = 72f;
        }

        return ( ThreadLocalRandom.current().nextFloat() * ( max - min ) ) + min;
    }

    private static float randomHumidVal( MqttSensor sensor )
    {
        float max = 80f;
        float min = 74f;

        if ( sensor.getId().contains("basement") )
        {
            max = 32f;
            min  = 30f;
        }
        else if ( sensor.getId().contains("office") )
        {
            max = 42f;
            min = 38f;
        }
        else if ( sensor.getId().contains("bedroom") )
        {
            max = 47f;
            min = 46f;
        }
        else if ( sensor.getId().contains("livingroom") )
        {
            max = 37f;
            min = 33f;
        }

        return ( ThreadLocalRandom.current().nextFloat() * ( max - min ) ) + min;
    }

    public static void main( String[] args ) {
        // Welcome Message
        System.out.println("### MQTT Messenger Bot, at your service! ###");
        System.out.println( "------------------------------" );
        System.out.println("Generating MQTT Workers...");

        // Thread Pool
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool( 5 );

        // MQTT Sensors
        MqttSensor bedroomStat    = new MqttSensor("bedroom_air_sensor" );
        MqttSensor livingroomStat = new MqttSensor("livingroom_air_sensor" );
        MqttSensor basementStat   = new MqttSensor("basement_air_sensor" );
        MqttSensor officeStat     = new MqttSensor("office_air_sensor" );
        MqttSensor prototype      = new MqttSensor( "prototype_weather_station" );

        // Threads
        executor.submit( new Mqtt5Worker( bedroomStat ) );
        executor.submit( new Mqtt5Worker( livingroomStat ) );
        executor.submit( new Mqtt5Worker( basementStat ) );
        executor.submit( new Mqtt5Worker( officeStat ) );
        executor.submit( new Mqtt3Worker( prototype ));

        executor.shutdown();
    }

    private static class Mqtt3Worker implements MqttWorker
    {
        private MqttSensor sensor;

        private final Mqtt3BlockingClient client;

        Mqtt3Worker( MqttSensor sensor)
        {
            this.sensor = sensor;
            this.client = Mqtt3Client.builder()
                    .identifier( sensor.getId() )
                    .serverHost( brokerHost )
                    .buildBlocking();
        }

        @Override
        public void run()
        {
            try
            {
                System.out.println( "Mqtt v3 Worker " + sensor.getId() + " starting..." );
                client.connect();

                Thread.sleep( SHORT_SLEEP );

                if ( !client.getState().isConnected() )
                {
                    System.out.println( "Client " + sensor.getId() + " disconnected unexpectedly!" );
                    return;
                }

                for ( int i = 0; i < 10000; i++ ) {
                    ObjectMapper mapper = new ObjectMapper();
                    sensor.clearMeasures();
                    sensor.addMeasure( "Outside Air Temperature", "°F", randomTempVal( sensor ) );
                    sensor.addMeasure( "Outside Air Humidity", "%", randomHumidVal( sensor ) );
                    sensor.addMeasure( "Wind Speed", "mph",  (ThreadLocalRandom.current().nextFloat() * ( 10f - 4f ) ) + 4f );

                    client.publishWith()
                            .topic( "prototype/" + sensor.getId() )
                            .qos( MqttQos.AT_MOST_ONCE )
                            .payload( mapper.writeValueAsBytes(sensor) )
                            .send();

                    Thread.sleep( LONG_SLEEP );
                }

                client.disconnect();
                System.out.println( "Worker " + sensor.getId() + " finished!");
            }
            catch (InterruptedException | JsonProcessingException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private static class Mqtt5Worker implements MqttWorker
    {
        private MqttSensor sensor;

        private final Mqtt5BlockingClient client;

        Mqtt5Worker( MqttSensor sensor)
        {
            this.sensor = sensor;
            this.client = Mqtt5Client.builder()
                .identifier( sensor.getId() )
                .serverHost( brokerHost )
                .buildBlocking();
        }

        @Override
        public void run()
        {
            try
            {
                System.out.println( "Mqtt v5 Worker " + sensor.getId() + " starting..." );
                client.connect();

                Thread.sleep( SHORT_SLEEP );

                if ( !client.getState().isConnected() )
                {
                    System.out.println( "Client " + sensor.getId() + " disconnected unexpectedly!" );
                    return;
                }

                for ( int i = 0; i < 10000; i++ ) {
                    ObjectMapper mapper = new ObjectMapper();
                    sensor.clearMeasures();
                    sensor.addMeasure( "Temperature", "°F", randomTempVal( sensor ) );
                    sensor.addMeasure( "Humidity", "%", randomHumidVal( sensor ));

                    client.publishWith()
                            .topic( "test/" + sensor.getId() )
                            .qos( MqttQos.AT_MOST_ONCE )
                            .payload( mapper.writeValueAsBytes(sensor) )
                            .send();

                    Thread.sleep( LONG_SLEEP );
                }

                client.disconnect();
                System.out.println( "Worker " + sensor.getId() + " finished!");
            }
            catch (InterruptedException | JsonProcessingException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}