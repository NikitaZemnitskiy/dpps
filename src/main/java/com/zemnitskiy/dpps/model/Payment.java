package com.zemnitskiy.dpps.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** Domain model representing a single payment transaction. Stored in the Ignite cache keyed by {@code id}. */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Payment implements Serializable, Binarylizable {

    @Serial
    private static final long serialVersionUID = 2L;

    private String id;
    private LocalDateTime dateTime;
    private String sender;
    private String receiver;
    private double value;

    @Override
    public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        writer.writeString("id", id);
        writer.writeString("dateTime", dateTime != null ? dateTime.toString() : null);
        writer.writeString("sender", sender);
        writer.writeString("receiver", receiver);
        writer.writeDouble("value", value);
    }

    @Override
    public void readBinary(BinaryReader reader) throws BinaryObjectException {
        id = reader.readString("id");
        String dt = reader.readString("dateTime");
        dateTime = dt != null ? LocalDateTime.parse(dt) : null;
        sender = reader.readString("sender");
        receiver = reader.readString("receiver");
        value = reader.readDouble("value");
    }
}
