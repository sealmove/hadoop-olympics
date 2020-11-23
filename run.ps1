<# Αναστασία Τσιλεπή 2022 2015 00179 dit15179
 # Στέφανος Μανδαλάς 2022 2017 00107 dit17107 #>

$project = "Olympics"
hdfs dfsadmin -safemode leave
hadoop fs -rm -r $outpath
hadoop jar "$project.jar" "$project" input output
$respath = "results"
If (!(Test-Path "$respath")) {mkdir "$respath"}
hadoop fs -cat "$outpath/*" > "results\$(get-date -f dd-MM-yy_HH-mm-ss).txt"