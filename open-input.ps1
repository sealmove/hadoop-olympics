<# Αναστασία Τσιλεπή 2022 2015 00179 dit15179
 # Στέφανος Μανδαλάς 2022 2017 00107 dit17107 #>

Remove-Item -ErrorAction Ignore input.csv
hadoop fs -get /user/sealmove/input/athlete_events.csv input.csv
Import-Csv input.csv | Out-GridView