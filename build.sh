#!/bin/bash

# Kinesis Streams to Firehose
# 
# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

version=`cat package.json | grep version | cut -d: -f2 | sed -e "s/\"//g" | sed -e "s/ //g" | sed -e "s/\,//g"`

functionName=lambda-streams-to-firehose
filename=$functionName-$version.zip
region=eu-west-1

npm install

rm $filename 2>&1 >> /dev/null

zip -r $filename index.js package.json node_modules/ README.md LICENSE && mv -f $filename dist/$filename

if [ "$1" = "true" ]; then
  aws lambda update-function-code --function-name $functionName --zip-file fileb://dist/$filename --region $region
fi
