#!/bin/sh

#
# Copyright 2014 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

#
# Common environment settings for Tephra.
# Uncomment the lines below, where needed, and modify to adapt to your environment.
#

# A string representing this instance of the Tephra server. $USER by default.
export IDENT_STRING=$USER

# Where log files are stored.  /var/log by default.
export LOG_DIR=/tmp/tephra-$IDENT_STRING

# The directory where pid files are stored. /var/run by default.
export PID_DIR=/tmp

# Add any extra classes to the classpath
# export EXTRA_CLASSPATH

# Set the JVM heap size
# export JAVA_HEAPMAX=-Xmx2048m

# Extra Java runtime options.
# Below are what we set by default.  May only work with SUN JVM.
# For more on why as well as other possible settings,
# see http://wiki.apache.org/hadoop/PerformanceTuning
export OPTS="-XX:+UseConcMarkSweepGC"
