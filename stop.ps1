<# Αναστασία Τσιλεπή 2022 2015 00179 dit15179
 # Στέφανος Μανδαλάς 2022 2017 00107 dit17107 #>

Stop-Job -Name namenode
Stop-Job -Name datanode
Stop-Job -Name resourcemanager
Stop-Job -Name nodemanager

Remove-Job -Name namenode
Remove-Job -Name datanode
Remove-Job -Name resourcemanager
Remove-Job -Name nodemanager