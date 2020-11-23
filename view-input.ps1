<# Anastasia Tsilepi 2022 2015 00179 dit15179
 # Stefanos Mandalas 2022 2017 00107 dit17107 #>

# copy input file from hadoop locally and view it in a graphical grid
Remove-Item -ErrorAction Ignore input.csv
hadoop fs -get input/athlete_events.csv input.csv
Import-Csv input.csv | Out-GridView