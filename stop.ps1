Stop-Job -Name namenode
Stop-Job -Name datanode
Stop-Job -Name resourcemanager
Stop-Job -Name nodemanager

Remove-Job -Name namenode
Remove-Job -Name datanode
Remove-Job -Name resourcemanager
Remove-Job -Name nodemanager