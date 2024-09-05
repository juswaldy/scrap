#!/usr/bin/bash
pushd /home/tomcat/CloverDX/sandboxes/Vena_Prod/script
for dim in GL01 GL02 GL03 GL04 GL05 GL06 YEAR PERD SCNR FLEX01 FLEX02 FLEX03 FLEX04 MEASURE; do
	ruby Vena.rb ExportToFile hierarchy "new CoA prototype" "dimension.name = '${dim}'" /home/share/Vena/Prod/Import/Working/${dim}.csv
done
popd
