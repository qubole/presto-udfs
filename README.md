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
 
To create a release, the version has to be changed, compile, deploy and 
bump the development version.
 
    # Change version as per http://semver.org/
    mvn versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false
    git commit -m "Prepare release X.Y.Z" -a
    git tag -a X.Y.Z -a "A useful comment here"
    git push
    git push --tags
    # SSH to build machine if required
    #Deploy to Maven Central
    mvn deploy -P release
    #Set new development version.
    mvn versions:set -DnewVersion=X.Y.(Z+1)-SNAPSHOT -DgenerateBackupPoms=false
    git commit -m "Set Development Version to X.Y.(Z+1)" -a
 
