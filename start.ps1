<# Anastasia Tsilepi 2022 2015 00179 dit15179
 # Stefanos Mandalas 2022 2017 00107 dit17107 #>

Start-Job -Name namenode {hdfs namenode}
Start-Job -Name datanode {hdfs datanode}
Start-Job -Name resourcemanager {yarn resourcemanager}
Start-Job -Name nodemanager {yarn nodemanager}