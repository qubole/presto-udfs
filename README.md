<!--
{% comment %}
  Copyright (c) 2016. Qubole Inc
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
{% endcomment %}
-->
# Presto User-Defined Functions(UDFs)
Plugin for Presto to allow addition of user defined functions. The plugin simplifies the process of adding user functions to Presto.

## Plugging in Presto UDFs
The details about how to plug in presto UDFs can be found [here](https://www.qubole.com/blog/product/plugging-in-presto-udfs/?nabe=5695374637924352:1).

## Presto Version Compatibility

| Presto Version| Last Compatible Release|
| ------------- |:-------------:|
| _ver 0.157_      | current    |
| _ver 0.142_      | udfs-1.0.0 |
| _ver 0.119_      | udfs-0.1.3 |

## Implemented User Defined Functions
The repository contains the following UDFs implemented for Presto :

#### HIVE UDFs
* **DATE-TIME Functions**
 1. **to_utc_timestamp(timestamp, string timezone) -> timestamp** <br />
      Assumes given timestamp is in given timezone and converts to UTC (as of Hive 0.8.0). For example, to_utc_timestamp('1970-01-01 00:00:00','PST') returns 1970-01-01 08:00:00.
 2. **from_utc_timestamp(timestamp, string timezone) -> timestamp**<br />
      Assumes given timestamp is UTC and converts to given timezone (as of Hive 0.8.0). For example, from_utc_timestamp('1970-01-01 08:00:00','PST') returns 1970-01-01 00:00:00.
 3. **unix_timestamp() -> timestamp**<br />
      Gets current Unix timestamp in seconds.
 4. **year(string date) -> int**<br />
      Returns the year part of a date or a timestamp string: year("1970-01-01 00:00:00") = 1970, year("1970-01-01") = 1970.
 5. **month(string date) -> int**<br />
      Returns the month part of a date or a timestamp string: month("1970-11-01 00:00:00") = 11, month("1970-11-01") = 11.
 6. **day(string date) -> int**<br />
      Returns the day part of a date or a timestamp string: day("1970-11-01 00:00:00") = 1, day("1970-11-01") = 1.
 7. **hour(string date) -> int**<br />
      Returns the hour of the timestamp: hour('2009-07-30 12:58:59') = 12, hour('12:58:59') = 12.
 8. **minute(string date) -> int**<br />
      Returns the minute of the timestamp: minute('2009-07-30 12:58:59') = 58, minute('12:58:59') = 58.
 9. **second(string date) -> int**<br />
      Returns the second of the timestamp: second('2009-07-30 12:58:59') = 59, second('12:58:59') = 59.
 10. **to_date(string timestamp) -> string**<br />
      Returns the date part of a timestamp string: to_date("1970-01-01 00:00:00") = "1970-01-01"
 11. **weekofyear(string date) -> int**<br />
      Returns the week number of a timestamp string: weekofyear("1970-11-01 00:00:00") = 44, weekofyear("1970-11-01") = 44.
 12. **date_sub(string startdate, int days) -> string**<br />
      Subtracts a number of days to startdate: date_sub('2008-12-31', 1) = '2008-12-30'.
 13. **date_add(string startdate, int days) -> string**<br />
      Adds a number of days to startdate: date_add('2008-12-31', 1) = '2009-01-01'.
 14. **datediff(string enddate, string startdate) -> string**<br />
      Returns the number of days from startdate to enddate: datediff('2009-03-01', '2009-02-27') = 2.
 15. **format_unixtimestamp(bigint unixtime[, string format]) -> string**<br />
      Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the format of "1970-01-01 00:00:00" unless a format string is specified. If a format string is specified the epoch time is converted in the specified format. More information about the formatter can be found [here](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html).<br />
      _**NOTE :** Due to name collision of presto 0.142's implementaion of `from_unixtime(bigint unixtime)` function, which returns the value as a timestamp type and Hive's `from_unixtime(bigint unixtime[, string format])` function, which returns the value as string type and supports formatter, the hive UDF has been implemented as `format_unixtimestamp(bigint unixtime[, string format])`._

* **MATH Functions**
 1. **pmod(INT a, INT b) -> INT, pmod(DOUBLE a, DOUBLE b) -> DOUBLE**<br />
      Returns the positive value of a mod b: pmod(17, -5) = -3.
 2. **rands(INT seed) -> DOUBLE**<br />
      Returns a random number (that changes from row to row) that is distributed uniformly from 0 to 1. Specifying the seed will make sure the generated random number sequence is deterministic: rands(3) = 0.731057369148862 <br />
      _**NOTE :** Due to name collision of presto 0.142's implementaion of `rand(int a)` function, which returns a number between 0 to a and Hive's `rand(int seed)` function, which sets the seed for the random number generator, the hive UDF has been implemented as `rands(int seed)`._
 3. **bin(BIGINT a) -> STRING**<br />
      Returns the number in binary format: bin(100) = 1100100.
 4. **hex(BIGINT a) -> STRING, hex(STRING a) -> STRING, hex(BINARY a) -> STRING**<br />
      If the argument is an INT or binary, hex returns the number as a STRING in hexadecimal format. Otherwise if the number is a STRING, it converts each character into its hexadecimal representation and returns the resulting STRING:  hex(123) = 7b, hex('123') = 7b, hex('1100100') = 64.
 5. **unhex(STRING a) -> BINARY**<br />
      Inverse of hex. Interprets each pair of characters as a hexadecimal number and converts to the byte representation of the number: unhex('7b') = 1111011.

* **STRING Functions**
 1. **locate(string substr, string str[, int pos]) -> int** <br />
      Returns the position of the first occurrence of substr in str after position pos: locate('si', 'mississipi', 2) = 4, locate('si', 'mississipi', 5) = 7
 2. **find_in_set(string str, string strList) -> int** <br />
      Returns the first occurance of str in strList where strList is a comma-delimited string. Returns null if either argument is null. Returns 0 if the first argument contains any commas:  find_in_set('ab', 'abc,b,ab,c,def') returns 3.
 3. **instr(string str, string substr) -> int** <br />
      Returns the position of the first occurrence of substr in str. Returns null if either of the arguments are null and returns 0 if substr could not be found in str: instr('mississipi' , 'si') = 4.

* **CONDITIONAL Functions**
  1. **nvl(T value, T default_value) -> T**<br/>
      ** Supported only till v1.0.0 due to the limitations presto new versions of Presto puts on plugins
      Returns default value if value is null else returns value: nvl(3,4) = 3, nvl(NULL,4) = 4.

* **MISCELLANEOUS Functions**
  1. **hash(a1[, a2...]) -> int**<br/>
      ** Supported only till v1.0.0 due to the limitations presto new versions of Presto puts on plugins
      Returns a hash value of the arguments. hash('a','b','c') = 143025634.

## Adding User Defined Functions to Presto-UDFs
 Functions can be added using annotations, follow https://prestodb.io/docs/0.157/develop/functions.html for details on how to add functions

  ** Note that Code generated functions were supported only till v1.0.0 due to the limitations presto new versions of Presto puts on plugins

## Release a new version of presto-udfs
Releases are always created from `master`. During development, `master`
has a version like `X.Y.Z-SNAPSHOT`.

    # Change version as per http://semver.org/
    mvn release:prepare -Prelease
    mvn release:perform -Prelease
    git push
    git push --tags
