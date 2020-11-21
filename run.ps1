$project = "Olympics"
$outpath = "/user/sealmove/output"
hdfs dfsadmin -safemode leave
hadoop fs -rm -r $outpath
hadoop jar "$project.jar" "$project" input output
$respath = "results"
If (!(Test-Path "$respath")) {mkdir "$respath"}
hadoop fs -cat "$outpath/*" > "results\$(get-date -f dd-MM-yy_HH-mm-ss).txt"