package com.collins.customcsvreader;

import java.util.Map;

import org.apache.nifi.serialization.record.RecordSchema;

import lombok.Data;

@Data
public class FileDescriptor
{
    private final RecordSchema schema;
    private final Map<String, String> attributes;
    private final boolean headerIncluded;
    private final boolean isQuoted;
    private final char crdFieldSeparator;
    private final char nestedRecordSeparator;
    private final char nestedRecordValueSeparator;
    private final char nestedPrimitiveListSeparator;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;
}

