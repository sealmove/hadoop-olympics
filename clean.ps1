<# Anastasia Tsilepi 2022 2015 00179 dit15179
 # Stefanos Mandalas 2022 2017 00107 dit17107 #>

. .\conf.ps1

Remove-Item -ErrorAction Ignore -Recurse $target
Remove-Item -ErrorAction Ignore *.jar
Remove-Item -ErrorAction Ignore *.csv