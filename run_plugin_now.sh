#!/bin/bash

echo "************* Deleting Old Files *************"
rm -rv ${HOME}/Applications/pdi/data-integration/plugins/kettle-quill*

echo "************* Copying New Files *************"
cp -v target/kettle-quill*.zip "${HOME}/Applications/pdi/data-integration/plugins/"

echo "************* Unzipping New Files *************"
cd "${HOME}/Applications/pdi/data-integration/plugins/"
unzip kettle-quill*.zip

echo "************* Launching Pentaho *************"
"${HOME}/Applications/pdi/data-integration/spoon.sh"
