<# Αναστασία Τσιλεπή 2022 2015 00179 dit15179
 # Στέφανος Μανδαλάς 2022 2017 00107 dit17107 #>

$project = "Olympics"
Remove-Item -ErrorAction Ignore "$project.jar"
Remove-Item -ErrorAction Ignore -Recurse "$project"
mkdir "$project"
javac -Xlint:deprecation -cp $(hadoop classpath) -d "$project" "$project.java"
jar -cvf "$project.jar" -C "$project/" .