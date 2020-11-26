<# Anastasia Tsilepi 2022 2015 00179 dit15179
 # Stefanos Mandalas 2022 2017 00107 dit17107 #>

. .\conf.ps1

# Remove old target files
Remove-Item -ErrorAction Ignore -Recurse "$target"
mkdir "$target\$project"

# Compile classes
javac -cp "$(hadoop classpath)" -d "$target\$project" "$src\$project.java"

# Archive classes in a jar file
jar -cvf "$target\$project\$project.jar" -C "$target\$project" .