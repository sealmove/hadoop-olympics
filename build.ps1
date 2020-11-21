$project = "Olympics"
Remove-Item -ErrorAction Ignore "$project.jar"
Remove-Item -ErrorAction Ignore -Recurse "$project"
mkdir "$project"
javac -Xlint:deprecation -cp $(hadoop classpath) -d "$project" "$project.java"
jar -cvf "$project.jar" -C "$project/" .