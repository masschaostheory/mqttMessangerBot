package com.masschaostheory;

import java.time.Instant;
import java.util.Date;

@SuppressWarnings("unused")
public interface MqttWorker extends Runnable {

    int LONG_SLEEP = 10000;
    int SHORT_SLEEP = 300;

    default void logConnect( int id, String uid )
    {
        System.out.println( "[ Worker-" + id +" ] Connecting client [ " + uid + " ]");
    }

    default void logDisconnect( int id, String uid )
    {
        System.out.println( "[ Worker-" + id +" ] Disconnecting client [ " + uid + " ]");
    }

    default void logPublish( int id )
    {
        System.out.println( "[ " + Date.from(Instant.now()) + " ]" + " Publishing from MQTT Worker-" + id );
    }

}
