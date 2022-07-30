# Introduction 

This nifi custom csv reader processes non-standard csv fields with nested values that is currently not supported by
the standard nifi csv reader.
It also provides hashing capability for sensitive data through schema configuration.
The reader contains all the capabilities already contained in the standard csv reader.

###  Getting Started

The project requires JDK8 and maven 3 to compile.

### Build and Test

execute `mvn clean install` inside the root directory

### Nifi deploy

copy the `nifi-CustomReader-nar-1.0.nar` and `nifi-CustomReader-api-nar-1.0.nar` into nifi lib directory and restart 
apache nifi

### how to
Once you have started your apache nifi successfully  you can access the custom reader through controller services. The following are screen images that can help you setup  

![](images/image1.png)

![](images/image2.png)

![](images/image3.png)
