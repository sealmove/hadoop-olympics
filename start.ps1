Start-Job -Name namenode {hdfs namenode}
Start-Job -Name datanode {hdfs datanode}
Start-Job -Name resourcemanager {yarn resourcemanager}
Start-Job -Name nodemanager {yarn nodemanager}