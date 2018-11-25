# Ambari Cluster Blueprint Extractor 
Script to extract a blueprint with Host mapping from a working Ambari Cluster.

Please use Python 2.7 since I didn't debug it using Python 3.

##### Usage:
```bash
ambari_blueprint_extractor.py -fp ~/path/to/store/blueprint -cn ClusterName -ah ambari.server.your.domain -au admin -ap admin_pass -dh database.server.your.domain -du ambari_user -dp ambari_password -dn ambari -bh ssh.bounce(tunnel).host -bu user -bk /home/user/your_key.pem 
```

#### Output:
```bash
Blueprint downloaded: /tmp/ClusterName.json
Generating cluster host mapping........
Extracted hosts from Ambari Database
Service mapping from Ambari database, which components run on each host
Host mapping generated: /tmp/ClusterName_map.json
```

 
