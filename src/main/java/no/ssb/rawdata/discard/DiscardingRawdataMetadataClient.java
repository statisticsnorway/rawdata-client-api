package no.ssb.rawdata.discard;

import no.ssb.rawdata.api.RawdataMetadataClient;

import java.util.Collections;

public class DiscardingRawdataMetadataClient implements RawdataMetadataClient {

    final String topic;

    public DiscardingRawdataMetadataClient(String topic) {
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Iterable<String> keys() {
        return Collections.emptyList();
    }

    @Override
    public byte[] get(String key) {
        return null;
    }

    @Override
    public DiscardingRawdataMetadataClient put(String key, byte[] value) {
        return this;
    }
}
