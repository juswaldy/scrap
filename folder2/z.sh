#!/usr/bin/bash

action=$1

################################################################################
## Unzip and get wc stats.
################################################################################
if [[ $action == "unzip_wc" ]]; then
	pushd /archive/clover/VAERS
	for z in *.zip; do
		ymd=`echo $z | sed 's/\(.*\)\.\(.*\)\.zip/\2/'`
		echo "$z ==> $ymd"
		mkdir $ymd
		unzip $z -d $ymd
		pushd $ymd
		wc *.csv > ../wc_output/${ymd}.txt
		popd
		rm -rf $ymd
	done
	popd

################################################################################
## Restart td pull if dead.
################################################################################
elif [[ $action == "tdpull" ]]; then

	pushd /home/jus/bin
	while :; do
		for x in ull_feeds; do
			pattern="[p]${x}"
			count=`ps aux | grep "$pattern" | wc -l`
			if [[ $count -eq 0 ]]; then
				echo "######################################## $pattern"
				y=0
				max=9
				i=$max
				while [ $i -gt 0 ]; do
					count=`ps aux | grep "$pattern" | wc -l`
					if [[ $count -eq 0 ]]; then
						y=$((y+1))
					fi
					i=$((i-1))
					sleep 1
				done
				if [[ $y -eq 9 ]]; then
					task="p${x}"
					echo "######################################## Restart $task"
					td.rb $task &
				fi
			fi
		done
		sleep 60
	done
	popd

################################################################################
## Generate gl trees.
################################################################################
elif [[ $action == "gl" ]]; then
	pushd /home/tomcat/CloverDX/sandboxes/Vena_Prod/script
	today=`date +'%Y-%m-%d'`
	infolder=/home/share/Vena/Prod/Import/Archive/`date +'%Y-%m'`
	outfolder=/home/share/IT/Prod/Import

	for level in Account Department; do
		inputfile=${infolder}/Vena_${level}_Hierarchy.csv
		outputfile=${outfolder}/Tree_GL_${level}_${today}.txt
		echo $inputfile to $outputfile
		ruby Vena.rb Venahierarchy2Edgelist $inputfile $outputfile $level
	done
	popd

################################################################################
## Generate gl sankeys.
################################################################################
elif [[ $action == "sankey" ]]; then
	cp -rp /home/jus/*.json /opt/tomcat_clover/webapps/apps/IT/FTP/GLDigits/colors
	mv -f /home/jus/*.json /opt/tomcat_clover/webapps/apps/IT/FTP/GLDigits/movable

################################################################################
## Download Vena hierarchies.
################################################################################
elif [[ $action == "venahier" ]]; then
	modelname=BudgetForecast_TEST_March28
	modelname="Budget & Forecast"
	modelname="Budget & Forecast TEST"
	modelname="BudgetForecast_TEST_March28"
	modelname="BudgetForecast_TEST_June20"
	modelname="BudgetForecast_PROD"
	pushd /home/tomcat/CloverDX/sandboxes/Vena_Prod/script
	#for dim in Year Period Scenario Company Fund Department Account SubAccount Position Employee Student Course Location Measure; do
	for dim in GL01 GL02 GL03 GL04 GL05 GL06 YEAR PERD SCNR FLEX01 FLEX02 FLEX03 FLEX04 MEASURE; do
		ruby ./Vena.rb ExportToFile hierarchy "${modelname}" "dimension.name = '${dim}'" "/home/share/Vena/Prod/Import/Working/golive/${dim}.csv"
	done
	popd

################################################################################
## Download VAERS data.
################################################################################
elif [[ $action == "vaers" ]]; then
	pushd /home/jus/bin
	date
	./VAERS.rb download
	popd

################################################################################
## Profile any found spreadsheet.
################################################################################
elif [[ $action == "profiler" ]]; then
	rootdir=/home/share/IT/Prod/Profiler/
	instances=`ls -1 /tmp/$action-* 2>/dev/null|wc -l`
	if [[ $instances -eq 0 ]]; then
		singleton=$action-`uuidgen`
		touch /tmp/$singleton
		for f in $rootdir/*; do
			if [[ $f == ${rootdir}/*.[Cc][Ss][Vv] || $f == ${rootdir}/*.[Xx][Ll][Ss][Xx] ]]; then
				date
				filename=`basename $f`
				/home/jus/bin/profiler.py $filename
				rm $f
			fi
		done
		rm /tmp/$singleton
	fi

################################################################################
fi

