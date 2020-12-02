<# Anastasia Tsilepi 2022 2015 00179 dit15179
 # Stefanos Mandalas 2022 2017 00107 dit17107 #>

. .\conf.ps1

$date = "$(get-date -f dd-MM-yy_HH-mm-ss)"
$file = "$results\$project\$date.txt"

# remove previous output to write new one
hdfs dfsadmin -safemode leave
hadoop fs -rm -r temp
hadoop fs -rm -r output

# run the code
hadoop jar "$target\$project\$project.jar" "$project" $in $out

if ($?) {
  # copy results from hdfs locally
  If (!(Test-Path "$results\$project")) {mkdir "$results\$project"}
  hadoop fs -cat "$out/*" > $file
}