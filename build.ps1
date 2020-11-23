<# Αναστασία Τσιλεπή 2022 2015 00179 dit15179
 # Στέφανος Μανδαλάς 2022 2017 00107 dit17107 #>

. .\conf.ps1

Remove-Item -ErrorAction Ignore -Recurse "$target"
mkdir "$target\$project"
javac -cp $(hadoop classpath) -d "$target\$project" "$src\$project.java"
jar -cvf "$target\$project\$project.jar" -C "$target\$project" .