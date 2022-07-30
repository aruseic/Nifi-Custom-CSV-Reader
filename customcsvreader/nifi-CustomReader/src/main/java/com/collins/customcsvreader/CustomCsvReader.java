package com.collins.customcsvreader;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.NonCloseableInputStream;

@Tags({ "record", "reader", "delimited", "values" })
@CapabilityDescription("Parses record formatted data, "
 + "arent well formed and can't be parsed correctly using the nifi csv record reader hence the custom  csv reader. "
 + "They also can have nested records within the fields "
 + "By default the reader assumes that the first line in the record is the header line"
)
public class CustomCsvReader extends SchemaRegistryService implements RecordReaderFactory{

    private static final AllowableValue TRUE = new AllowableValue("true", "true");
    private static final AllowableValue FALSE = new AllowableValue("false");

    private static final PropertyDescriptor HEADER = new PropertyDescriptor.Builder()
        .name("header")
        .displayName("Has Header")
        .allowableValues(TRUE, FALSE)
        .defaultValue(TRUE.getValue())
        .required(true)
        .description("Specifies whether or not the record source file has a header line")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    private static final PropertyDescriptor FIELD_SEPARATOR = new PropertyDescriptor.Builder()
        .name("fieldSeparator")
        .displayName("Field Separator")
        .defaultValue("|")
        .required(true)
        .description("record field separator")
        .allowableValues("|", ",")
        .addValidator(new SeparatorValidator(new char[] { '|', ',' }))
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    private static final PropertyDescriptor NESTED_RECORD_SEPARATOR = new PropertyDescriptor.Builder()
        .name("nestedRecordSeparator")
        .displayName("Nested Record Separator")
        .defaultValue(";")
        .required(true)
        .description("Separator for nested records within a record field")
        .allowableValues(";")
        .addValidator(new SeparatorValidator(new char[] { ';' }))
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    private static final PropertyDescriptor NESTED_RECORD_VALUES_SEPARATOR = new PropertyDescriptor.Builder()
        .name("nestedRecordFieldSeparator")
        .displayName("Nested Record Values Separator")
        .defaultValue(",")
        .required(true)
        .description("Separator for fields within the nested records")
        .allowableValues(",", "=", ":")
        .addValidator(new SeparatorValidator(new char[] { ',', '=', ':' }))
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    private static final PropertyDescriptor NESTED_PRIMITIVE_LIST_ITEM_SEPARATOR = new PropertyDescriptor.Builder()
        .name("nestedPrimitiveListItemSeparator")
        .displayName("Primitive List Item Separator")
        .defaultValue(",")
        .required(true)
        .description("Separator for list of primitive items e.g. int, long, date")
        .allowableValues(",", "|")
        .addValidator(new SeparatorValidator(new char[] { ',', '|' }))
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    private static final PropertyDescriptor QUOTED = new PropertyDescriptor.Builder()
        .name("quoted")
        .displayName("Fields Are Quoted")
        .allowableValues(TRUE, FALSE)
        .defaultValue(TRUE.getValue())
        .required(true)
        .description("Specifies whether or not the record fields are quoted. Only well-formed records are supported for the unqoted otherwise you'll encounter weird errors")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    private static final PropertyDescriptor SKIP_MALFORMED_LINES = new PropertyDescriptor.Builder()
        .name("skipMalformedLines")
        .displayName("Skip Malformed Lines")
        .allowableValues(TRUE, FALSE)
        .defaultValue(FALSE.getValue())
        .required(true)
        .description("Specifies whether the reader will fail the file on malformed lines or continue. A warning will be logged for each malformed line. You can override by setting the flow file attribute customcsv.skipmalformedlines")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    private volatile char separator, nestedRecordSeparator, nestedRecordValuesSeparator,
        nestedPrimitiveListSeparator;

    private volatile boolean hasHeader, quoted, skipMalformedLines;
    private volatile String dateFormat, timeFormat, timestampFormat;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> parentProps = super.getSupportedPropertyDescriptors();
        ArrayList<PropertyDescriptor> properties = new ArrayList<>(parentProps.size() + 8);
        properties.addAll(parentProps);
        properties.add(HEADER);
        properties.add(QUOTED);
        properties.add(FIELD_SEPARATOR);
        properties.add(NESTED_RECORD_SEPARATOR);
        properties.add(NESTED_RECORD_VALUES_SEPARATOR);
        properties.add(NESTED_PRIMITIVE_LIST_ITEM_SEPARATOR);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        properties.add(SKIP_MALFORMED_LINES);
        return Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void configure(final ConfigurationContext context){
        super.storeSchemaAccessStrategy(context);
        separator = context.getProperty(FIELD_SEPARATOR).getValue().charAt(0); //get the first value
        nestedRecordSeparator =  context.getProperty(NESTED_RECORD_SEPARATOR).getValue().charAt(0);
        nestedRecordValuesSeparator = context.getProperty(NESTED_RECORD_VALUES_SEPARATOR).getValue().charAt(0);
        nestedPrimitiveListSeparator = context.getProperty(NESTED_PRIMITIVE_LIST_ITEM_SEPARATOR).getValue().charAt(0);

        hasHeader = context.getProperty(HEADER).asBoolean();
        quoted = context.getProperty(QUOTED).asBoolean();
        dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
        skipMalformedLines = context.getProperty(SKIP_MALFORMED_LINES).asBoolean();
    }

    @Override
    public Collection<ValidationResult> customValidate(final ValidationContext validationContext){
        Collection<ValidationResult> superResults = super.customValidate(validationContext);
        String fieldSeparator = validationContext.getProperty(FIELD_SEPARATOR).getValue();
        String nestedRecValueSeparator = validationContext.getProperty(NESTED_RECORD_VALUES_SEPARATOR).getValue();
        String nestedPrimListSeparator = validationContext.getProperty(NESTED_PRIMITIVE_LIST_ITEM_SEPARATOR).getValue();
        List<ValidationResult> results = new ArrayList<>(superResults.size() + 2);
        results.addAll(superResults);
        results.add(new ValidationResult.Builder()
            .valid(!fieldSeparator.equalsIgnoreCase(nestedRecValueSeparator)) //not valid if they equal
            .subject("Field Separator equalTo Record Value Separator")
            .explanation("The record field separator should not be equal to the nested record value separator")
            .input(nestedRecValueSeparator)
            .build()
            );
        results.add(new ValidationResult.Builder()
            .valid(!fieldSeparator.equalsIgnoreCase(nestedPrimListSeparator))
            .subject("Field Separator equalTo Nested Primitive Value Separator")
            .explanation("The record field separator should not be equal to the nested primitive list value separator")
            .input(nestedPrimListSeparator)
            .build()
        );
        return results;
    }


    @Override
	public RecordReader createRecordReader(Map<String, String> variables, InputStream in,long l, ComponentLog logger)
			throws MalformedRecordException, IOException, SchemaNotFoundException {
        RecordSchema schema = getSchema(variables, new NonCloseableInputStream(in), null);
        
        ReadOptions readOptions = new ReadOptions().skipMalformedLines(getFinalSkipValue(variables));
        FileDescriptor descriptor = new FileDescriptor(
            schema, 
            variables, 
            hasHeader, 
            quoted, 
            separator, 
            nestedRecordSeparator, 
            nestedRecordValuesSeparator, 
            nestedPrimitiveListSeparator, 
            dateFormat,
            timeFormat,
            timestampFormat
            );
        return new CsvCustomRecordReader(in, descriptor, readOptions, logger);
    }
    
    private boolean getFinalSkipValue(Map<String,String> variables)
    {
        boolean skip = skipMalformedLines;
        if(variables != null)
        {
            String skipValue = variables.get("customcsv.skipmalformedlines");
            if(skipValue != null && !skipValue.isEmpty())
            {
                try
                {
                    skip = Boolean.parseBoolean(skipValue);
                }
                catch(Exception ex)
                {
                    skip = skipMalformedLines;
                }
            }
        }
        return skip;
    }

}