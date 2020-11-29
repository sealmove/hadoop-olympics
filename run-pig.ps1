<# Anastasia Tsilepi 2022 2015 00179 dit15179
 # Stefanos Mandalas 2022 2017 00107 dit17107 #>

. .\conf.ps1

$date = "$(get-date -f dd-MM-yy_HH-mm-ss)"
$file = "$results\$project\${date}_pig.txt"

hdfs dfsadmin -safemode leave
hadoop fs -rm -r $pig_out
pig "$pig_src\$project.pig"
hadoop fs -cat "$pig_out/*" > $file