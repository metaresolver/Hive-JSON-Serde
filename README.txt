JsonSerde - a read/write SerDe for JSON Data
AUTHOR: Roberto Congiu <rcongiu@yahoo.com>

Serialization/Deserialization module for Apache Hadoop Hive

This module allows hive to read and write in JSON format (see http://json.org for more info).

Features:
* Read data stored in JSON format
* Convert data to JSON format when INSERT INTO table
* arrays and maps are supported
* nested data structures are also supported.
* can rename columns, or extract nested JSON to a top level column
* bad JSON is ignored

COMPILE

Use maven to compile the serde.

EXAMPLES

Example scripts with simple sample data are in src/test/scripts. Here some excerpts:

* Query with complex fields like arrays

CREATE TABLE json_test1 (
	one boolean,
	three array<string>,
	two double,
	four string )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;

* Nested JSON on top level column (underscore method)
CREATE TABLE moo (
       outer_inner_field string
) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
  WITH SERDEPROPERTIES ('underscores-are-paths'='true')

* Nested JSON on top level column (Amazon style Path method)
CREATE TABLE moo (
       field string
) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
  WITH SERDEPROPERTIES ('paths'='outer.inner.field')

* Nested structures

You can also define nested structures:
add jar ../../../target/json-serde-1.0-SNAPSHOT-jar-with-dependencies.jar;

CREATE TABLE json_nested_test (
	country string,
	languages array<string>,
	religions map<string,array<int>>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;

-- data : {"country":"Switzerland","languages":["German","French","Italian"],"religions":{"catholic":[10,20],"protestant":[40,50]}}
LOAD DATA LOCAL INPATH 'nesteddata.txt' OVERWRITE INTO TABLE  json_nested_test ;

select * from json_nested_test;  -- result: Switzerland	["German","French","Italian"]	{"catholic":[10,20],"protestant":[40,50]}
select languages[0] from json_nested_test; -- result: German
select religions['catholic'][0] from json_nested_test; -- result: 10

* MALFORMED DATA

The default behavior on malformed data is throwing an exception.
For example, for malformed json like
{"country":"Italy","languages" "Italian","religions":{"catholic":"90"}}

you get:
Failed with exception java.io.IOException:org.apache.hadoop.hive.serde2.SerDeException: Row is not a valid JSON Object - JSONException: Expected a ':' after a key at 32 [character 33 line 1]

this may not be desirable if you have a few bad lines you wish to ignore. If so you can do:
ALTER TABLE json_table SET SERDEPROPERTIES ( "ignore.malformed.json" = "true");

it will not make the query fail, and the above record will be returned as
NULL	null	null


* ARCHITECTURE

For the JSON encoding/decoding, I am using a modified version of Douglas Crockfords JSON library: https://github.com/douglascrockford/JSON-java which is included in the distribution. I had to make some minor changes to it, for this reason I included it in my distribution and moved it to another package (since it's included in hive!)

The SerDe builds a series of wrappers around JSONObject. Since serialization and deserialization are executed for every (and possibly billions) record we want to minimize object creation, so instead of serializing/deserializing to an ArrayList, I kept the JSONObject and built a cached objectinspector around it. So when deserializing, hive gets a JSONObject, and a JSONStructObjectInspector to read from it. Hive has Structs, Maps, Arrays and primitives while JSON has Objects, Arrays and primitives.Hive Maps and Structs are both implemented as object, which are less restrictive than hive maps: a JSON Object could be a mix of keys and values of different types, while hive expects you to declare the type of map (example: map<string,string>). The user is responsible for having the JSON data structure match hive table declaration.

More detailed explanation on my blog:
http://www.congiu.com/articles/json_serde


* THANKS

Thanks to Douglas Crockford for the liberal license for his JSON library, and thanks to my employer OpenX and my boss Michael Lum for letting me open source the code.




