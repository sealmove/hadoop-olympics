$project = "Olympics"
hdfs dfsadmin -safemode leave
hadoop fs -rm -r /user/sealmove/output
hadoop jar "$project.jar" "$project" input output
$outpath = "results"
If (!(Test-Path "$outpath")) {mkdir "$outpath"}
hadoop fs -cat /user/sealmove/output/* > "results\$(get-date -f dd-MM-yy_HH-mm-ss).txt"