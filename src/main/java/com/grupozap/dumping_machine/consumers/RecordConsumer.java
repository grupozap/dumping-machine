package com.grupozap.dumping_machine.consumers;

import com.grupozap.dumping_machine.formaters.AvroExtendedMessage;

public interface RecordConsumer {
    void write(AvroExtendedMessage record);

    void close();

    void delete();
}