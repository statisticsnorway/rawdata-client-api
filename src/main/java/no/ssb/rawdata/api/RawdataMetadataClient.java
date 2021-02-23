package no.ssb.rawdata.api;

public interface RawdataMetadataClient {

    /**
     * @return the name of the topic from which this metadata-client will read from and write to.
     */
    String topic();

    /**
     * @return an iterable with the keys of all the metadata entries stored for this topic.
     */
    Iterable<String> keys();

    /**
     * Read value for a given key.
     *
     * @param key the key of the metadata entry.
     * @return the value of the metadata entry.
     */
    byte[] get(String key);

    /**
     * Write metadata value for a given key.
     *
     * @param key   the key of the metadata entry
     * @param value the value of the metadata entry
     * @return this metadata-client to allow fluent-style.
     */
    RawdataMetadataClient put(String key, byte[] value);
}
