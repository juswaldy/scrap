#!/usr/bin/bash
entity=$1
id=$2
scriptfolder=/home/tomcat/CloverDX/sandboxes/IT_Prod/script
targetfolder=/home/share/IT/Prod/Import/Working
pushd $scriptfolder
ruby IT.rb PullTeamDynamix $entity $id
chown jus:jus ${targetfolder}/${entity}.json
mv ${targetfolder}/${entity}.json /tmp
popd
