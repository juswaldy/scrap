#!/usr/bin/bash
userhost=jus@its80601d
identityfile=/home/jus/.ssh/id_rsa
targetfolder=/home/jus/notebook/jus/dw/profiling/Retention
tempfile=/tmp/x.html
pushd /opt/tomcat_clover/webapps/apps/IT/Profiling/Retention
#ssh $userhost -q -i $identityfile "/home/jus/bin/job.sh retention"
#printf '%s\n' "cd $targetfolder" 'mget *' | sftp -q -i $identityfile -b - $userhost
rm index.html
echo '<style>table,th,td{border:1px solid gray;border-collapse:collapse;padding:5px;}</style><table><thead><th>Summaries</th><th>Size</th></thead>' > $tempfile
ls -lh *.html | cut -b25-29,43- | sed 's/\s*\(.*\) \(.*\)/<tr><td><a href="\2">\2<\/a><\/td><td>\1<\/td><\/tr>/' >> $tempfile
echo '</table><br/><table><thead><th>Details</th><th>Size</th></thead>' >> $tempfile
ls -lh *.json | cut -b25-29,43- | sed 's/\s*\(.*\) \(.*\)/<tr><td><a href="\2">\2<\/a><\/td><td>\1<\/td><\/tr>/' >> $tempfile
echo '</table>' >> $tempfile
mv $tempfile index.html
popd
