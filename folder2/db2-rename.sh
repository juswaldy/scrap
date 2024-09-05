#!/usr/bin/bash
pushd /home/tomcat/CloverDX/sandboxes
tempfile=/tmp/db2rename.txt
for f in `find . -type f \( -iname '*.cfg' -o -iname '*.rb' \) -exec grep -sil 'db2' {} \;` ; do
  echo $f
  sed 's/db2/prod-aqdb/g' $f > $tempfile
  mv $tempfile $f
done
popd
