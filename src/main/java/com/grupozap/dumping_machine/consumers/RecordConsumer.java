package com.grupozap.dumping_machine.consumers;

import com.grupozap.dumping_machine.formaters.AvroExtendedMessage;

import java.io.IOException;

public interface RecordConsumer {
    void write(AvroExtendedMessage record) throws IOException;

    void close() throws IOException;

    void delete() throws IOException;
}