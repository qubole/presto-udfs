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

Release a new version of presto-udfs
====================================

Releases are always created from `master`. During development, `master` 
has a version like `X.Y.Z-SNAPSHOT`. 
 
    # Change version as per http://semver.org/
    mvn release:prepare -Prelease
    mvn release:perform -Prelease
    git push
    git push --tags

