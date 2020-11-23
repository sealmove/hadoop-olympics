<# Αναστασία Τσιλεπή 2022 2015 00179 dit15179
 # Στέφανος Μανδαλάς 2022 2017 00107 dit17107 #>

. .\conf.ps1

Remove-Item -ErrorAction Ignore -Recurse "$target"
mkdir "$target"
javac -cp $(hadoop classpath) -d "$target" "$src\$project.java"
jar -cvf "$target\$project.jar" -C "$target\" .