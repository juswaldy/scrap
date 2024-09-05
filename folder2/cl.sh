#!/bin/bash


prefix=WE_00D15000000Frw2EAC
date=20230624
max=639

rootdir=/archive/clover/ERx
pushd $rootdir

## Extract ZIPs.
# for f in *.ZIP; do
# 	dirname="${f%.*}"
# 	mkdir -p $dirname
# 	7za x -o${dirname} $f
# done

## Remove binaries.
for d in `ls -d $rootdir/${prefix}_*_${date}`; do
	for t in Attachments ContentVersion Documents "Other Uploaded Collateral"; do
		backupdir="$d/$t"
		if [[ -d "$backupdir" ]]; then
			pushd "$d/$t"
			rm -f *
			popd
		fi
	done
done

## Make tarball of backups, and clean up.
tar cvf ${prefix}_${max}_${date}.tar ${prefix}_[0-9]*_${date}
gzip -f -9 ${prefix}_${max}_${date}.tar
rm -rf ${prefix}_[0-9]*_${date} ${prefix}_[0-9]*_${date}.ZIP
chown tomcat:tomcat ${prefix}_${max}_${date}.tar.gz

popd

