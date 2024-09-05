#!/usr/bin/bash
userhost=jus@its80601d
identityfile=/home/jus/.ssh/id_rsa
targetfolder=/home/jus/sqlparser
targetfile=SQLParser2.tar.gz

pushd /opt/tomcat_clover/webapps/apps/IT
ssh $userhost -q -i $identityfile "/home/jus/bin/job.sh sqlparser2"
printf '%s\n' "cd $targetfolder" "get $targetfile" | sftp -q -i $identityfile -b - $userhost
sudo chown tomcat:tomcat $targetfile
tar zxvf $targetfile
rm -f $targetfile
popd
