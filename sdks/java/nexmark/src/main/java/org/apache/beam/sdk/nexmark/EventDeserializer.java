package org.apache.beam.sdk.nexmark;

import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

public class EventDeserializer implements Deserializer<Event> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Event deserialize(String s, byte[] bytes) {
        final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try {
            return Event.CODER.decode(bis);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
