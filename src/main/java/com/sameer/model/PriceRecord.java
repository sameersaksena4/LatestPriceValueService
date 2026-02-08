package com.sameer.model;

import java.time.LocalDateTime;
import java.util.Objects;

public class PriceRecord {

    private final String id;
    private final LocalDateTime asOf;
    private final Object payload;

    public PriceRecord(String id, LocalDateTime asOf, Object payload) {
        this.id = id;
        this.asOf = asOf;
        this.payload = payload;
    }

    public String getId() {
        return id;
    }

    public LocalDateTime getAsOf() {
        return asOf;
    }

    public Object getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PriceRecord that = (PriceRecord) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(asOf, that.asOf)
                && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, asOf, payload);
    }

    @Override
    public String toString() {
        return "PriceRecord{" +
                "id='" + id + '\'' +
                ", asOf=" + asOf +
                ", payload=" + payload +
                '}';
    }
}
