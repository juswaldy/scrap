#!/usr/bin/bash
userhost=jus@its80601d
identityfile=/home/jus/.ssh/id_rsa
targetfolder=/home/jus/notebook/jus/dw/profiling/Assets
pushd /opt/tomcat_clover/webapps/apps/IT/Assets
ssh $userhost -q -i $identityfile "python /home/jus/bin/dwprofiler_assets.py --targetfolder $targetfolder"
printf '%s\n' "cd $targetfolder" 'mget *' | sftp -q -i $identityfile -b - $userhost
popd
