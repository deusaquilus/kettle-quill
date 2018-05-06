#!/bin/bash

mvn clean package;

pdi_path=
pdi_plugin_path="$pdi_path/plugins/"

echo "************* Deleting Old Files *************"
rm -rv "${pdi_plugin_path}/kettle-quill*"

echo "************* Copying New Files *************"
cp -v target/kettle-quill*.zip "$pdi_plugin_path"

echo "************* Unzipping New Files *************"
cd "$pdi_plugin_path"
unzip kettle-quill*.zip

echo "************* Launching Pentaho *************"
"${pdi_path}/Data Integration.app/Contents/MacOS/JavaApplicationStub"
