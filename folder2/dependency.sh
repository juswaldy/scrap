#!/usr/bin/bash

environment=$1
scripthome=/home/tomcat/CloverDX/sandboxes/TWU
dependency_script=./dependency.py
targetfolder=/tmp
targetfiles=${targetfolder}/Tree*.txt
vizhome=/opt/tomcat_clover/webapps/apps/IT/SQLParser1
waitseconds=60

pushd $scripthome
rm -f $targetfiles

# Version 1.
for params in "Deps|" "Callers|--reverse"; do
	connection="jz_${environment}"
	target=`echo $params | cut -d'|' -f1`
	addlparam=`echo $params | cut -d'|' -f2`
	echo "Processing $connection $target All"
	python ${dependency_script} --connection $connection --targetfolder $targetfolder ${addlparam}
	mv $targetfiles /home/share/IT/Prod/Import
	sleep $waitseconds
	pushd $vizhome/$target/TmsEPrd
	sed 's/tree.json/all_tree.json/;s/treeselect.js/all_treeselect.js/' index.html > all_index.html
	sed 's/tree.json/all_tree.json/' tree.html > all_tree.html
	sed 's/tree.json/all_tree.json/' treemap.html > all_treemap.html
	cp tree.json all_tree.json
	popd
	echo "Processing $connection $target"
	python ${dependency_script} --connection $connection --targetfolder $targetfolder --pattern 'TWU_%' ${addlparam}
	mv $targetfiles /home/share/IT/Prod/Import
	for db in aq ics fr; do
		connection="${db}_${environment}"
		echo "Processing $connection $target"
		python ${dependency_script} --connection $connection --targetfolder $targetfolder ${addlparam}
		mv $targetfiles /home/share/IT/Prod/Import
		sleep $waitseconds
	done
done

# Version 2.
touch /home/share/IT/Prod/Import/SQLParser2.download

popd
