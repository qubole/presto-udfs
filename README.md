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
#Presto User-Defined Functions(UDFs)
Plugin for Presto to allow addition of user defined functions. The plugin simplifies the process of adding user functions to Presto. 

##Plugging in Presto UDFs
The details about how to plug in presto UDFs can be found [here](https://www.qubole.com/blog/product/plugging-in-presto-udfs/?nabe=5695374637924352:1).

##Presto Version Compatibility

| Presto Version| Last Compatible Release|
| ------------- |:-------------:|
| _ver 0.142_      | Current |
| _ver 0.119_      | udfs-0.1.3      |

##Implemented User Defined Functions
The repository contains the following UDFs implemented for Presto : 

####HIVE UDFs
* **DATE-TIME Functions**
 1. **to_utc_timestamp(timestamp, string timezone) -> timestamp** <br />Assumes given timestamp is in given timezone and converts to UTC (as of Hive 0.8.0). For example, to_utc_timestamp('1970-01-01 00:00:00','PST') returns 1970-01-01 08:00:00. 
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

* **MATH Functions**
 1. **pmod(INT a, INT b) -> INT, pmod(DOUBLE a, DOUBLE b) -> DOUBLE**<br />
    Returns the positive value of a mod b.
 2. **rands(INT seed) -> DOUBLE**<br />
    Returns a random number (that changes from row to row) that is distributed uniformly from 0 to 1. Specifying the seed will make sure the generated random number sequence is deterministic. <br />_**NOTE :** Due to name collision of presto 0.142's implementaion of rand(int a) function, which returns a number between 0 to a and Hive's rand(int seed) function, which sets the seed for the random number generator, the hive UDF has been implemented as rands(int seed)._ 

* **STRING Functions**
 1. **locate(string substr, string str[, int pos]) -> int** <br />Returns the position of the first occurrence of substr in str after position pos: locate('si', 'mississipi', 2) = 4, locate('si', 'mississipi', 5) = 7
 2. **find_in_set(string str, string strList) -> int** <br />Returns the first occurance of str in strList where strList is a comma-delimited string. Returns null if either argument is null. Returns 0 if the first argument contains any commas. For example, find_in_set('ab', 'abc,b,ab,c,def') returns 3.
 


##Release a new version of presto-udfs
Releases are always created from `master`. During development, `master` 
has a version like `X.Y.Z-SNAPSHOT`. 
 
    # Change version as per http://semver.org/
    mvn release:prepare -Prelease
    mvn release:perform -Prelease
    git push
    git push --tags
