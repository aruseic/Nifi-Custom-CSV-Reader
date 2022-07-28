# Introduction 

Extensions we have build to work with NiFi. Currently we have the following extensions:

1. `cdr-reader`: Implements a nifi `RecordReader` and `RecordReaderFactory` that we use to read CDR files
2. `execute-database-record`: Processor that executes a given sql statement once per record, the sql statement can reference record fields. For example we can execute the query `update dbo.users set name=@name where id=@id`, the processor will parse the query and create a JDBC compliant equivalent and at the same time pass in parameters that correspond to `Record.getValue(name)` and `Record.getValue(id)`
3 `cdridentifier`: Abandoned effort at generating 64-bit cdr file identifiers when we thought MapR would be charging per TB, not maintained anymore

# Getting Started

The project requires JDK8 and maven 3 to compile.

# Build and Test

CD into the directory and run `mvn clean install` assuming you have maven globally installed 

# Contribute

Think about your life decisions 1st, then checkout out articles such as

1. https://www.nifi.rocks/developing-a-custom-apache-nifi-processor-unit-tests-partI/
2. https://medium.com/hashmapinc/creating-custom-processors-and-controllers-in-apache-nifi-e14148740ea
3. https://www.nifi.rocks/developing-a-custom-apache-nifi-processor-json/

Not enough? JFGI. 

I'll update once I figure out Java dev, don't yet have build guidlines.