# Installation

* Copy the zip file to an Edge node with HBase client libs/configurations files installed 
* Update the environment dependencies in the shell script at [*HBase-RegionTool-project]/region-tool.sh*:


```
# Update to your current JAVA HOME
JAVA_HOME=/usr/java/jdk1.7.0_67

# Update to the Region Tool home folder
TOOL_HOME_DIR=/opt/HBase-RegionTool
```


# How to Debug

*  Use –report as argument:

``` 
./region-tool.sh -tablename testtable –report
```
 
*  Review the logs
 Implementation based in Apache Commons Logging / Log4j
 Default log configuration at *resources/tool-log4j.properties*

Default path at *TOOL_HOME_DIR/log/*

* Increase trace level
