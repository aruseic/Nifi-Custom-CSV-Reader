package com.collins.customcsvreader;

//import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.commons.text.*;

public class CsvCustomRecordReader implements RecordReader{

	static final RecordFieldType[] SIMPLE_TYPES = new RecordFieldType[]{
		RecordFieldType.BIGINT,
		RecordFieldType.BOOLEAN,
		RecordFieldType.BYTE,
		RecordFieldType.CHAR,
		RecordFieldType.DATE,
		RecordFieldType.DOUBLE,
		RecordFieldType.FLOAT,
		RecordFieldType.INT,
		RecordFieldType.LONG,
		RecordFieldType.SHORT,
		RecordFieldType.STRING,
		RecordFieldType.TIME,
		RecordFieldType.TIMESTAMP
	};

	private final InputStream in;
	private final BufferedReader reader;
	private final FileDescriptor descriptor;
	private final ReadOptions readOptions;
	private final ComponentLog log;
	private final Supplier<DateFormat> lazy_date_format, lazy_time_format, lazy_timestamp_format;

	private int lineNumber = 0, skippedRecordCount = 0;

	//logging attributes
	private final String fileName, uuid;
	private final Object[] attributeParam;

	public CsvCustomRecordReader(final InputStream in, FileDescriptor descriptor, ReadOptions readOptions, final ComponentLog log) throws IOException {
        super();
		this.in = in;
		this.reader = new BufferedReader(new InputStreamReader(in));
		this.descriptor = descriptor;
		this.readOptions = readOptions;
		this.log = log;
		this.lazy_date_format = getSupplierOrNull(descriptor.getDateFormat());
		this.lazy_time_format = getSupplierOrNull(descriptor.getTimeFormat());
		this.lazy_timestamp_format = getSupplierOrNull(descriptor.getTimestampFormat());

		if(descriptor.getAttributes() != null){
			this.fileName = descriptor.getAttributes().get("filename");
			this.uuid = descriptor.getAttributes().get("uuid");
			this.attributeParam = new Object[] { fileName, uuid };			
		} else{
			this.fileName = null;
			this.uuid = null;
			attributeParam = new Object[] { null, null };
		}

		if(descriptor.isHeaderIncluded()){
			reader.readLine();
			if(log.isInfoEnabled()){
				log.info("[{}][{}]Skipping header line because has header is set to true", attributeParam);
			}
			lineNumber++;
		}
	}
	
	@Override
	public void close() throws IOException {
		//log total number of lines skipped
		if(skippedRecordCount > 0 && log.isInfoEnabled()){
			log.info("[{}][{}] has skipped {} lines", new Object[] { fileName, uuid, skippedRecordCount });
		}
		reader.close();
		in.close();
	}

	@Override
	public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields)
			throws IOException, MalformedRecordException {

		while(true){
			String line = reader.readLine();
			if(line == null){
				return null; //we are @ end of record
			}
			lineNumber++; //ensure we are @ the correct line #
			if(log.isInfoEnabled()){
				log.info("[{}][{}] parsing line {}", new Object[] { fileName, uuid, lineNumber });
			}
			try{
				MapRecord record = parseCdr(line, coerceTypes, dropUnknownFields);
				return record;
			}
			catch(Exception ex){			
				if(readOptions.skipMalformedLines())
				{
					if(log.isWarnEnabled()){
						log.warn("[{}][{}] skipping line {} because it was malformed", new Object[] { fileName, uuid, lineNumber }, ex);
					}
					skippedRecordCount++;					
				} else {
					//if we aren't skipping malformed lines then throw
					if(log.isErrorEnabled()){
						log.error("[{}][{}] error parsing line {}", new Object[] { fileName, uuid, lineNumber }, ex);
					}
					throw ex; //rethrow the error
				}
			}
		}
	}

	protected MapRecord parseCdr(String cdr, boolean coerceTypes, boolean dropUnknownFields) 
		throws MalformedRecordException{
		
		Map<String, Object> values = new HashMap<>(descriptor.getSchema().getFieldCount() * 2);
		RecordSchema schema = getSchema();
		boolean warnExtraFields = true;

		int currentChar=0, startOfNextField = 0, schemaIndex = 0;

		for(; currentChar < cdr.length(); currentChar++){ //get only the fields we understand from the schema ignore the rest
			boolean separatorHit = cdr.charAt(currentChar) == descriptor.getCrdFieldSeparator();
			if(descriptor.isQuoted()){
				//if we are quoted then we need to check that the previous and next characters
				//are quotes
				separatorHit = separatorHit && cdr.charAt(currentChar -1) == '"' && cdr.charAt(currentChar + 1) == '"';
			}			
			boolean eol = currentChar == cdr.length() - 1; //end of line
            if(separatorHit || eol){
				if(schemaIndex < schema.getFieldCount()){
					RecordField field = schema.getField(schemaIndex);
					String fieldValue = null;

					if(descriptor.isQuoted()){
						fieldValue = cdr.substring(startOfNextField + 1, eol ? currentChar : currentChar -1);//we do prev + 1 & idx - 1 to remove the quotes
					} else {
						fieldValue = cdr.substring(startOfNextField, separatorHit ? currentChar : currentChar + 1);
					}

					Object coercedValue = convertOrThrow(field, fieldValue, coerceTypes, schemaIndex);
					//Use if to check if the name contains PII then Pseudo anonymize the data
					/*
					* if(field.getFieldName().contains _PII then hash)
					*
					* */

					if(field.getFieldName().toUpperCase().endsWith("PII")){
						String sha256hexCorcedvalue = DigestUtils.sha256Hex(coercedValue.toString());
						values.put(field.getFieldName(), sha256hexCorcedvalue);
					}else {
						values.put(field.getFieldName(), coercedValue);
					}

					//values.put(field.getFieldName(), coercedValue);

					startOfNextField = currentChar + 1;
					schemaIndex++;
				} else {
					if(log.isWarnEnabled() && warnExtraFields){
						log.warn("[{}][{}] The file has more cdr fields than expected", attributeParam);
						warnExtraFields = false; //we just want to log this once per file.
					}
					break; //drop unknown fields by force
				}                
            }
		}
		addOrThrowOnMissingSchemaFields(schemaIndex, values);
		
		return new MapRecord(schema, values);
	}

	//top level method will either convert the text to the required type of throw a MalformedRecordException
	private Object convertOrThrow(RecordField field, String fieldValue, boolean coerceTypes, int fieldIndex)
		throws MalformedRecordException{
		if(!field.isNullable() && (fieldValue == null || fieldValue.isEmpty())){
			throw new MalformedRecordException(
					String.format("The field %s at schema index %d is not nullable but the content at line %d is null",
						field.getFieldName(), fieldIndex, lineNumber
						)
				);
		}

		RecordFieldType fieldType = field.getDataType().getFieldType();
		
		if(isSimpleType(fieldType)){
			Object coercedValue = coerceTypes 
				? convertSimple(fieldValue, field.getDataType(), field.getFieldName())
				: fieldValue;
			return coercedValue;
		} else if(fieldType == RecordFieldType.ARRAY){
			ArrayDataType arrayType = (ArrayDataType)field.getDataType(); //must use field.getDataType not fieldType.getDataType otherwise elementType is null
			DataType elementType = arrayType.getElementType() == null ? RecordFieldType.STRING.getDataType() : arrayType.getElementType();
			if(isSimpleType(elementType.getFieldType())){
				return getSimpleTypeArray(field, fieldValue, elementType, coerceTypes);
			} else if(elementType.getFieldType() == RecordFieldType.RECORD) {
				//array of records
				RecordDataType recordType = (RecordDataType)elementType;
				RecordSchema childSchema = recordType.getChildSchema();
				List<Record> src = parseNested(fieldValue, childSchema, field, fieldIndex, coerceTypes);
				return toRecordArray(src);
			}
			throw new RuntimeException("not implemented");
		} else if(fieldType == RecordFieldType.RECORD){
			final RecordDataType recordType = (RecordDataType)field.getDataType(); //must use field.getDatatype not fieldType.getDataType otherwise childSchema is null
			final RecordSchema childSchema = recordType.getChildSchema();
			if(childSchema == null){
				throw new RuntimeException(
					String.format("The nested schema for field %s is missing", field.getFieldName())
				);
			}
			List<Record> parsed = parseNested(fieldValue, childSchema, field, fieldIndex, coerceTypes);
			return parsed.size() == 0 ? null : parsed.get(0);
		} else if(fieldType == RecordFieldType.MAP){
			final MapDataType valueType = (MapDataType)field.getDataType();
			//create a schema of key/value so that we can use the same method as RecordArray.
			//we'll convert that to a map afterwards.
			final RecordSchema childSchema = new SimpleRecordSchema(Arrays.asList(
				new RecordField[]{
					new RecordField("key", RecordFieldType.STRING.getDataType(), false),
					new RecordField("value", valueType.getValueType())
				}
			));
			List<Record> records = parseNested(fieldValue, childSchema, field, fieldIndex, coerceTypes);
			Map<String, Object> map = new HashMap<>(records.size());
			for(int i=0; i<records.size(); i++){
				Record r = records.get(i);
				map.put(r.getAsString("key"), r.getValue("value"));
			}
			return map;
		}
		throw new RuntimeException("not a supported type " + fieldType);
	}

	//if the array is of a simple type then call this method
	//should work for ints, longs, date etc that don't need further string parsing
	protected Object[] getSimpleTypeArray(final RecordField field, final String fieldText, final DataType elementType, boolean coerceTypes)
		throws MalformedRecordException{
		if(fieldText == null || fieldText.isEmpty()){
			return null;
		}
		
		List<Object> converted = new ArrayList<>(fieldText.length());

		for(int i=0, prev = -1; i<fieldText.length(); i++){
			if(fieldText.charAt(i) == descriptor.getNestedPrimitiveListSeparator() || i == fieldText.length() - 1){
				//get the value 
				String value = fieldText.substring(prev + 1, i == fieldText.length() - 1 ? i + 1 : i).trim(); //trim to remove spaces
				//if our value is just spaces then skip this is a null that we don't want to add to the array
				if(value.isEmpty()){
					if(log.isWarnEnabled()){
						log.warn("The array in field {} of line {} with text {} has a null {} beween index {} and {}",
							new Object[]
							{
								field.getFieldName(),
								lineNumber,
								fieldText,
								elementType.getFieldType().toString(),
								Integer.toString(prev + 1),
								Integer.toString(i == fieldText.length() - 1 ? i + 1 : i)
							}
						);
					}
				} else {
					//add the value to our items collection;
					converted.add(
						coerceTypes 
							? convertSimple(value, elementType, field.getFieldName())
							: value
					);
				}

				//set prev to last value upper index
				prev = i;
			}
		}
		//remove unnecessary nulls
		if(converted.size() > 0){
			converted.removeIf(p -> p == null);
		}

		if(converted.size() == 0 && field.isNullable()){
			return null;
		}

		return converted.toArray();
	}

	/*
	* Retrieves a nested record. A CDR field, especially snapshots, can contain a composite field
	* which we need to unwrap.
	*/
	protected List<Record> parseNested(String cdrFieldText, final RecordSchema nestedSchema, 
		final RecordField cdrRecordInfo, int cdrFieldIndex, boolean coerceTypes)
		throws MalformedRecordException {
			int startOfNextField = 0, schemaIndex = 0;
			String value = null;
			Map<String, Object> properties = new HashMap<>(nestedSchema.getFieldCount() * 2);
			final List<Record> records = new ArrayList<>(8);
			
			//bug in html encoded entities
			if(descriptor.getNestedRecordSeparator() == ';' && cdrFieldText.indexOf('&') > -1){
				//html decode the text, because we might have
				//html encoded text.
				cdrFieldText = StringEscapeUtils.unescapeHtml4(cdrFieldText);
			}

			for(int currentPos=startOfNextField; currentPos < cdrFieldText.length(); currentPos++){
				if(cdrFieldText.charAt(currentPos) == descriptor.getNestedRecordSeparator() && properties.size() == 0){
					//This deals with null records, not sure exactly how this happens but in the CDR
					//we end up with a something like ;; where ; is the nested record separator. We nee
					//to reset.
					startOfNextField = currentPos+1; //prev is after separator
					schemaIndex = 0;
					continue;
				}	

				if(cdrFieldText.charAt(currentPos) == descriptor.getNestedRecordValueSeparator() 
					|| cdrFieldText.charAt(currentPos) == descriptor.getNestedRecordSeparator() || currentPos == cdrFieldText.length() - 1){

					if(schemaIndex < nestedSchema.getFieldCount()){
						//get the value text.
						//As it happens, you can have something like abc,efg,; where , is the field separator & ; the record separator
						//This occurs when the field is null and as such we need to handle the split so that the ,; records a valid null
						//valued field. Since the upper index on substring is exclusive, if the char at length-1 is the nested record value
						//separator i.e. ','' in this case, currentPos == startOfNextField so we add 1 to currentPos to get an empty string.
						value = cdrFieldText.substring(startOfNextField, 
							currentPos == cdrFieldText.length() - 1 && cdrFieldText.charAt(currentPos) != descriptor.getNestedRecordValueSeparator() 
								? currentPos + 1 : currentPos
							).trim();

						RecordField field = nestedSchema.getField(schemaIndex);
						startOfNextField = currentPos+1;
						schemaIndex ++;

						try{
							//we have the schema add the key value
							Object obj = convertOrThrow(field, value, coerceTypes, cdrFieldIndex);
							properties.put(field.getFieldName(), obj);
							if(cdrFieldText.charAt(currentPos) == descriptor.getNestedRecordSeparator() || currentPos == cdrFieldText.length() - 1){
								records.add(new MapRecord(nestedSchema, properties));
								properties = new HashMap<>(nestedSchema.getFieldCount() * 2); //start again
								schemaIndex = 0; //reset schema index
							}
						}
						catch(MalformedRecordException e){
							if(log.isErrorEnabled()){
								//log this error
								log.error("[{}][{}] unable to convert line {} field {} with value {} into data type {}",
									new Object[] { fileName, uuid, lineNumber, field.getFieldName(), value, field.getDataType().getFieldType().name() },
									e);
							}
							throw e;
						}
					} else if(cdrFieldText.charAt(currentPos) == descriptor.getNestedRecordSeparator()){
						//this here deals with extra fields, we need to keep advancing
						//so that we can retrieve the next records
						startOfNextField = currentPos+1; //prev is after separator
						schemaIndex = 0;
					}
				}
			}
			return records;
	}

	//convert to simple type
	protected Object convertSimple(final String value, final DataType dataType, final String fieldName)
		throws MalformedRecordException{
		if(dataType == null || value == null || value.isEmpty()){
			return null;
		}
		switch(dataType.getFieldType()){
			case STRING:
				return value;
			case BOOLEAN:
			case INT:
			case LONG:
			case FLOAT:
			case DOUBLE:
			case BYTE:
			case CHAR:
			case SHORT:
				if(DataTypeUtils.isCompatibleDataType(value, dataType)){
					return DataTypeUtils.convertType(value, dataType, lazy_date_format, lazy_time_format, lazy_timestamp_format, fieldName);
				}
				break;
			case DATE:
				if(DataTypeUtils.isDateTypeCompatible(value, lazy_date_format == null ? null : descriptor.getDateFormat())){
					return DataTypeUtils.toDate(value, lazy_date_format, fieldName);
				}
				break;
			case TIME:
				if(DataTypeUtils.isTimeTypeCompatible(value, lazy_time_format == null ? null : descriptor.getTimeFormat())){
					return DataTypeUtils.toTime(value, lazy_time_format, fieldName);
				}
				break;
			case TIMESTAMP:
				if(DataTypeUtils.isTimestampTypeCompatible(value, lazy_timestamp_format == null ? null : descriptor.getTimestampFormat())){
					return DataTypeUtils.toTimestamp(value, lazy_timestamp_format, fieldName);
				}
				break;
		}
		throw new MalformedRecordException(
			String.format("Could not convert value %s to the simple data type %s for field %s", value, dataType.getFieldType(), fieldName)
		);
	}

	//If there are missing schema fields in the file
	protected void addOrThrowOnMissingSchemaFields(final int currentSchemaIndex, final Map<String, Object> values)
		throws MalformedRecordException
	{
		//add null fields missing from the file if any otherwise throw
		for(int i=currentSchemaIndex;i < descriptor.getSchema().getFieldCount(); i++)
		{
			RecordField field = descriptor.getSchema().getField(i);
			if(field.isNullable())
			{
				if(log.isWarnEnabled())
				{
					log.warn("[{}][{}] Adding nullable field {} to record, eol was reached field wasn't part of the line {}",
						new Object[]
						{
							fileName,
							uuid,
							field.getFieldName(),
							Integer.toString(lineNumber)
						}
					);
				}
				values.put(field.getFieldName(), null);
			} else {
				if(log.isErrorEnabled())
				{
					log.error("[{}][{}] field {} is defined as non-nullable in the schema but eol was reached field wasn't part of the line {}",
						new Object[]
						{
							fileName,
							uuid,
							field.getFieldName(),
							Integer.toString(lineNumber)
						}
					);
				}
				convertOrThrow(field, null, false, i); //so that we get the same error message
			}
		}
	}

	@Override
	public RecordSchema getSchema() throws MalformedRecordException {
		return descriptor.getSchema();
	}


	private static Supplier<DateFormat> getSupplierOrNull(String format){
		if(format == null || format.isEmpty()){
			return null;
		}
		return () -> DataTypeUtils.getDateFormat(format);
	}



	private static boolean isSimpleType(RecordFieldType fieldType){
		for(int i=0; i<SIMPLE_TYPES.length; i++){
			if(SIMPLE_TYPES[i] == fieldType){
				return true;
			}
		}
		return false;
	}

	private static Record[] toRecordArray(List<Record> src){
        Record[] records = new Record[src.size()];
        for(int i=0; i<src.size(); i++){
            records[i] = (Record)src.get(i);
        }
        return records;
	}

}