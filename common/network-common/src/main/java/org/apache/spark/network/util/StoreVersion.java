package org.apache.spark.network.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.charset.StandardCharsets;

public class StoreVersion {

    static final byte[] KEY = "StoreVersion".getBytes(StandardCharsets.UTF_8);

    public final int major;
    public final int minor;

    @JsonCreator
    public StoreVersion(@JsonProperty("major") int major, @JsonProperty("minor") int minor) {
        this.major = major;
        this.minor = minor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StoreVersion that = (StoreVersion) o;

        return major == that.major && minor == that.minor;
    }

    @Override
    public int hashCode() {
        int result = major;
        result = 31 * result + minor;
        return result;
    }
}
