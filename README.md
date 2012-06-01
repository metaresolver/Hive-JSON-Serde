JsonSerde - a read/write SerDe for JSON Data
============================================
AUTHOR: Roberto Congiu <rcongiu@yahoo.com>

Serialization/Deserialization module for Apache Hadoop Hive

This module allows hive to read and write in JSON format (see http://json.org for more info).

Features:
---------
* Read data stored in JSON format
* Convert data to JSON format when INSERT INTO table
* arrays and maps are supported
* nested data structures are also supported.
* can rename columns, or extract nested JSON to a top level column, by two methods (either an explicit path or with _'s in the field name). Renaming is also useful for JSON fields that are reserved words in Hive.
* bad JSON is ignored

COMPILE
-------
Use maven to compile the serde.

EXAMPLES
--------
Example scripts with simple sample data are in src/test/scripts. Here some excerpts:

* Query with complex fields like arrays

```sql
CREATE TABLE json_test1 (
	one boolean,
	three array<string>,
	two double,
	four string )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;
```

* Nested JSON on top level column (underscore method)
```sql
CREATE TABLE moo (
       outer_inner_field string
) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
  WITH SERDEPROPERTIES ('underscores-are-paths'='true')
```

* Nested JSON on top level column (Amazon style Path method)
```sql
CREATE TABLE moo (
       field string
) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
  WITH SERDEPROPERTIES ('paths'='outer.inner.field')
```

* Nested structures (explicitly defined in Hive method)
You can also define nested structures:
```sql
ADD JAR s3://foo/json-serde-1.1-SNAPSHOT-jar-with-dependencies.jar;
CREATE TABLE json_nested_test (
	country string,
	languages array<string>,
	religions map<string,array<int>>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;
```

MALFORMED DATA
--------------
The default behavior on malformed data is to return null for every column on that row.


ARCHITECTURE
------------
For the JSON encoding/decoding, Jackson is used.

The SerDe builds a series of wrappers around JSONObject. Since serialization and deserialization are executed for every (and possibly billions) record we want to minimize object creation, so instead of serializing/deserializing to an ArrayList, I kept the JSONObject and built a cached objectinspector around it. So when deserializing, hive gets a JSONObject, and a JSONStructObjectInspector to read from it. Hive has Structs, Maps, Arrays and primitives while JSON has Objects, Arrays and primitives.Hive Maps and Structs are both implemented as object, which are less restrictive than hive maps: a JSON Object could be a mix of keys and values of different types, while hive expects you to declare the type of map (example: map<string,string>). The user is responsible for having the JSON data structure match hive table declaration.

More detailed explanation on my blog:
http://www.congiu.com/articles/json_serde


THANKS
------
Thanks to Douglas Crockford for the liberal license for his JSON library, and thanks to my employer OpenX and my boss Michael Lum for letting me open source the code.




