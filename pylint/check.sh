#!/bin/sh


pylint="python /opt/local/Library/Frameworks/Python.framework/Versions/2.5/bin/pylint"

rm -rf results/*
mkdir results/resources

pythonFiles=$(find "../awspider" -type f -path "*py" )
for pythonFile in $pythonFiles
do	
	output=$(echo $pythonFile | sed "s/..\/awspider\///")
	$pylint --output-format=html --max-line-length=300 --good-names=d,s,pg,rq,s3,sdb --disable-msg=C0111 $pythonFile > "results/$output.html"
done

