#!/usr/bin/bash

sandboxroot=/tmp/sandbox
sandboxhome=$sandboxroot/sandboxes
configsroot=$sandboxroot/configs
scriptsroot=$sandboxroot/scripts
exampleshome=$sandboxroot/examples
mkdir -pv $configsroot
mkdir -pv $scriptsroot
mkdir -pv $exampleshome

# Copy sandboxes folder to $sandboxroot and go there to process it
echo "# Copy sandboxes folder to $sandboxroot and go there to process it"
cp -rpv /home/tomcat/CloverDX/sandboxes $sandboxroot

# Process each sandbox
echo "# Process each sandbox"
for s in ActiveNet AdAstraSyncCloud ADP ADPtoTD BPR-RCE ChqRec CoursesSync EFT-RCE ERxSync GLSync PAD-EFT RCE-GLI SalesforceSync Vena AppArmorSync Bookware Interfolio SymphonySync SchoolDataSync idProducerSync IT Orbis DigitalRecords; do
    # Processing $s
    echo "# Processing $s"

    # Remove Prod/PROD from the names
    echo "# Remove Prod/PROD from the names"
    mv -v $sandboxhome/${s}_[Pp][Rr][Oo][Dd] $sandboxhome/$s

    # Copy configs to the $configsroot folder and remove them from the sandboxes
    echo "# Copy configs to the $configsroot folder and remove them from the sandboxes"
    mkdir -pv $configsroot/$s
    cp -rpv $sandboxhome/$s/conn/* $configsroot/$s
    rm -v $sandboxhome/$s/conn/*

    # Move sandbox scripts to the $scriptsroot folder
    echo "# Move sandbox scripts to the $scriptsroot folder"
    if [ "$s" == "ChqRec" ]; then
        mv -v $sandboxhome/$s/scripts $scriptsroot/$s
    else
        mv -v $sandboxhome/$s/script $scriptsroot/$s
    fi

    # Move external script configs to the scripts folder
    echo "# Move external script configs to the scripts folder"
    mv -v $sandboxhome/$s/conn/*.rb $scriptsroot/$s

    # Remove all files from sandbox data folders
    echo "# Remove all files from sandbox data folders"
    echo "find $sandboxhome/$s/data-* -type f -delete"
    find $sandboxhome/$s/data-* -type f -delete

    # Remove all logs from sandbox script folders
    echo "# Remove all logs from sandbox script folders"
    echo "find $scriptsroot/$s -regex '.*\(txt\|log\).*$' -not -name '*.py' -delete"
    find $scriptsroot/$s -regex '.*\(txt\|log\).*$' -not -name '*.py' -delete

    # Remove compiled python files and pycache folders
    echo "# Remove compiled python files and pycache folders"
    echo "find $sandboxhome -type f -name '*.pyc' -delete"
    find $sandboxhome -type f -name '*.pyc' -delete
    echo "find $sandboxhome/$s -type d -name __pycache__ -exec rm -rf {} \;"
    find $sandboxhome/$s -type d -name __pycache__ -exec rm -rf {} \;
done

# Move external scripts to the $scriptsroot folder
echo "# Move external scripts to the $scriptsroot folder"
for d in Shared TWU; do
    mv -v $sandboxhome/$d $scriptsroot
done

# Remove dotfiles
echo "# Remove dotfiles"
echo "find $sandboxhome -regex '.*\/\..*' -delete"
find $sandboxhome -regex '.*\/\..*' -delete

# Remove Salesforce wsdls and backup folder
echo "# Remove Salesforce wsdls and backup folder"
rm -fv $sandboxhome/ERxSync/meta/enterprise.wsdl.*
rm -fv $sandboxhome/ERxSync/backup
rm -fv $sandboxhome/SalesforceSync/meta/enterprise.wsdl.*
rm -fv $sandboxhome/SalesforceSync/meta/metadata.wsdl
rm -fv $sandboxhome/SalesforceSync/meta/partner.wsdl
rm -fv $sandboxhome/SalesforceSync/meta/tooling.wsdl

# Remove miscellaneous files/dirs
echo "# Remove miscellaneous files/dirs"
rm -rfv $configsroot/CoursesSync/__pycache__
rm -rfv $scriptsroot/ERxSync/maps
rm -rfv $scriptsroot/TWU/log.txt
rm -rfv $scriptsroot/Vena/tmp
rm -rfv $sandboxhome/SalesforceSync/graph/reassign
rm -rfv $sandboxhome/PAD-EFT/sql
rm -rfv $sandboxhome/RCE-GLI/sql

# Chmod on sandbox folders and files
echo "# Chmod on sandbox folders and files"
echo "find $sandboxhome -type d -exec chmod 770 {} \;"
find $sandboxhome -type d -exec chmod 770 {} \;
echo "find $sandboxhome -type f -exec chmod 660 {} \;"
find $sandboxhome -type f -exec chmod 660 {} \;

# Rename non-canonical sandboxes
echo "# Rename non-canonical sandboxes"
find $sandboxroot -name 'AdAstraSyncCloud' -type d -exec bash -c 'mv "$1" "${1/AdAstraSyncCloud/AdAstra/}"' -- {} \;
find $sandboxroot -name 'AppArmorSync' -type d -exec bash -c 'mv "$1" "${1/AppArmorSync/AppArmor/}"' -- {} \;
find $sandboxroot -name 'SalesforceSync' -type d -exec bash -c 'mv "$1" "${1/SalesforceSync/Causeview/}"' -- {} \;
find $sandboxroot -name 'CoursesSync' -type d -exec bash -c 'mv "$1" "${1/CoursesSync/Courses/}"' -- {} \;
find $sandboxroot -name 'ERxSync' -type d -exec bash -c 'mv "$1" "${1/ERxSync/ERx/}"' -- {} \;
find $sandboxroot -name 'GLSync' -type d -exec bash -c 'mv "$1" "${1/GLSync/GL/}"' -- {} \;
find $sandboxroot -name 'idProducerSync' -type d -exec bash -c 'mv "$1" "${1/idProducerSync/idProducer/}"' -- {} \;
find $sandboxroot -name 'SchoolDataSync' -type d -exec bash -c 'mv "$1" "${1/SchoolDataSync/SDS/}"' -- {} \;
find $sandboxroot -name 'SymphonySync' -type d -exec bash -c 'mv "$1" "${1/SymphonySync/Symphony/}"' -- {} \;
find $sandboxroot -name 'ADPtoTD' -type d -exec bash -c 'mv "$1" "${1/ADPtoTD/TeamDynamix/}"' -- {} \;

# Move examples to their own folder
echo "# Move examples to their own folder"
for f in BasicExamples BigDataExamples DataQualityExamples default JobflowExamples; do
    mv -v $sandboxhome/$f $exampleshome
done

# Move script configs to the scripts folder
echo "# Move script configs to the scripts folder"
for f in ADP Causeview ERx IT Vena; do
    mv -v $configsroot/$f/*.rb $scriptsroot/$f
done
mv -v $configsroot/Courses/CoursesSync_config.rb $scriptsroot/Courses/Courses_config.rb
mv -v $configsroot/Courses/*.py $scriptsroot/Courses
mv -v $configsroot/Orbis/*.py $scriptsroot/Orbis

# Rename env specific configs
echo "# Rename env specific configs"
for s in Courses SDS Vena; do
    find $sandboxhome/$s -type f -name *.grf -exec grep -sl Jenzabar_PROD.cfg {} \; | while read f; do
        echo "sed -i 's/Jenzabar_PROD\.cfg/Jenzabar.cfg/g' \"$f\""
        sed -i 's/Jenzabar_PROD\.cfg/Jenzabar.cfg/g' "$f"
    done
done
find $sandboxhome/Vena -type f -name *.grf -exec grep -sl Fast_PROD.cfg {} \; | while read f; do
    echo "sed -i 's/Fast_PROD\.cfg/FRDB.cfg/g' \"$f\""
    sed -i 's/Fast_PROD\.cfg/FRDB.cfg/g' "$f"
done

# Replace env specific configs
echo "# Replace env specific configs"
echo "sed -i 's/ChqRec_Prod/ChqRec/g' $sandboxhome/ChqRec/graph/PositivePay.grf"
sed -i 's/ChqRec_Prod/ChqRec/g' $sandboxhome/ChqRec/graph/PositivePay.grf
echo "sed -i 's/ERxSync_Prod/ERx/g' $sandboxhome/ERx/graph/DownloadAppChanges.grf"
sed -i 's/ERxSync_Prod/ERx/g' $sandboxhome/ERx/graph/DownloadAppChanges.grf
echo "sed -i 's/ERxSync_Prod/ERx/g' $sandboxhome/ERx/graph/DownloadAppChanges2.grf"
sed -i 's/ERxSync_Prod/ERx/g' $sandboxhome/ERx/graph/DownloadAppChanges2.grf

# Rename share folders
echo "# Rename share folders"
echo "sed -i 's/home\/share\/Activenet\/Prod/mnt\/winshare\/ActiveNet/g' $sandboxhome/ActiveNet/workspace.prm"
sed -i 's/home\/share\/Activenet\/Prod/mnt\/winshare\/ActiveNet/g' $sandboxhome/ActiveNet/workspace.prm
echo "sed -i 's/home\/share\/BPR\/Prod/mnt\/winshare\/BPR/g' $sandboxhome/BPR-RCE/workspace.prm"
sed -i 's/home\/share\/BPR\/Prod/mnt\/winshare\/BPR/g' $sandboxhome/BPR-RCE/workspace.prm
echo "sed -i 's/home\/share\/BPR\/\${_Environment}/mnt\/winshare\/BPR/g' $sandboxhome/ChqRec/graph/PositivePay.grf"
sed -i 's/home\/share\/BPR\/\${_Environment}/home\/share\/BPR/g' $sandboxhome/ChqRec/graph/PositivePay.grf
echo "sed -i 's/home\/share\/ChqRec\/Prod/mnt\/winshare\/ChqRec/g' $sandboxhome/ChqRec/workspace.prm"
sed -i 's/home\/share\/ChqRec\/Prod/mnt\/winshare\/ChqRec/g' $sandboxhome/ChqRec/workspace.prm
echo "sed -i 's/home\/share\/BPR\/Prod/mnt\/winshare\/BPR/g' $sandboxhome/EFT-RCE/workspace.prm"
sed -i 's/home\/share\/BPR\/Prod/mnt\/winshare\/BPR/g' $sandboxhome/EFT-RCE/workspace.prm
echo "sed -i 's/home\/share\/BPR\/Prod/mnt\/winshare\/BPR/g' $sandboxhome/PAD-EFT/workspace.prm"
sed -i 's/home\/share\/BPR\/Prod/mnt\/winshare\/BPR/g' $sandboxhome/PAD-EFT/workspace.prm
echo "sed -i 's/home\/share\/BPR\/Prod/mnt\/winshare\/BPR/g' $sandboxhome/RCE-GLI/workspace.prm"
sed -i 's/home\/share\/BPR\/Prod/mnt\/winshare\/BPR/g' $sandboxhome/RCE-GLI/workspace.prm
echo "sed -i 's/home\/share\/Bookware\/Prod/mnt\/winshare\/Bookware/g' $sandboxhome/Bookware/workspace.prm"
sed -i 's/home\/share\/Bookware\/Prod/mnt\/winshare\/Bookware/g' $sandboxhome/Bookware/workspace.prm
echo "sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/00Scrap.grf"
sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/00Scrap.grf
echo "sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/00-CSV-DB.grf"
sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/00-CSV-DB.grf
echo "sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/JSON_Spreadsheet.grf"
sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/JSON_Spreadsheet.grf
echo "sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/TeamDynamix.Pull.grf"
sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/TeamDynamix.Pull.grf
echo "sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/TeamDynamix.Pull2.grf"
sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/TeamDynamix.Pull2.grf
echo "sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/Telus.Assets.Import.grf"
sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/Telus.Assets.Import.grf
echo "sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/VAERS.Load.grf"
sed -i 's/home\/share\/IT\/Prod/mnt\/winshare\/IT/g' $sandboxhome/IT/graph/VAERS.Load.grf

# Move non-secret parameter files back to the conn folder
echo "# Move non-secret parameter files back to the conn folder"
for s in ActiveNet GL idProducer Symphony; do
    mv -v $configsroot/$s/parameters.prm $sandboxhome/$s/conn
done

# Inject secrets parameter file into graphs
echo "# Inject secrets parameter file into graphs"
for s in ActiveNet AdAstra ADP AppArmor Bookware BPR-RCE Causeview ChqRec Courses DigitalRecords EFT-RCE ERx GL idProducer IT Orbis PAD-EFT RCE-GLI SDS Symphony TeamDynamix Vena; do
    find $sandboxhome/$s -type f -exec grep -sl '.cfg' {} \; | while read f; do
        echo sed -i 's/<GraphParameterFile fileURL="workspace.prm"\/>/<GraphParameterFile fileURL="workspace.prm"\/>\n<GraphParameterFile fileURL="conn\/misc.prm"\/>/g' "$f"
        sed -i 's/<GraphParameterFile fileURL="workspace.prm"\/>/<GraphParameterFile fileURL="workspace.prm"\/>\n<GraphParameterFile fileURL="conn\/misc.prm"\/>/g' "$f"
    done
done

# Change windows eol to unix
echo "# Change windows eol to unix"
find $sandboxhome -type f -not -regex '.*xls[x]*' -exec sed -i 's/\r//g' {} \;

# Chown for jus
echo "# Chown for jus"
chown -Rv jus:jus $sandboxroot

# Create file list for diff
cd /tmp/sandbox-current
find . | sort > /tmp/1.txt
cd /tmp/sandbox
find . | sort > /tmp/2.txt

echo "# Done"