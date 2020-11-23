<# Αναστασία Τσιλεπή 2022 2015 00179 dit15179
 # Στέφανος Μανδαλάς 2022 2017 00107 dit17107 #>

. .\conf.ps1

hdfs dfsadmin -safemode leave
hadoop fs -rm -r output
hadoop jar "$target\$project.jar" "$project" $in $out
If (!(Test-Path "$results")) {mkdir "$results"}
hadoop fs -cat "$out/*" > "$results\$(get-date -f dd-MM-yy_HH-mm-ss).txt"