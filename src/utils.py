import os
import tempfile

YBA_BASE_URL = os.environ.get("YBA_BASE_URL", "default_url")
YBA_USERNAME = os.environ.get("YBA_USERNAME", "default_user")
YBA_PASSWORD = os.environ.get("YBA_PASSWORD", "default_password")
CUSTOM_KEYPAIR_PATH = "/path/to/keypair"
UNIVERSE_CREATE_TEMPLATE = {}

MASTER_GFLAGS = {
    'callhome_enabled': 'false',
}
TSERVER_GFLAGS = {
    'callhome_enabled': 'false',
}


def get_tempfile(file_content):
    fd, path = tempfile.mkstemp(".crt")
    try:
        os.write(fd, file_content.encode())
        os.close(fd)
    except IOError:
        raise RuntimeError("Error creating temp cert file")
    return path


aws_1node_1az_rf1 = {
        "clusters": [
            {
                "uuid": "4b3f8a21-03d8-4950-b85f-ef408094596c",
                "index": 0,
                "regions": [
                    {
                        "code": "us-west-2",
                        "name": "US West (Oregon)",
                        "uuid": "211b14a7-156b-415c-b671-677454d3d5ae",
                        "zones": [
                            {
                                "code": "us-west-2a",
                                "name": "us-west-2a",
                                "uuid": "1bcdf140-e857-457e-a4f3-385eab0d60fe",
                                "active": True,
                                "subnet": "subnet-11798e59"
                            },
                            {
                                "code": "us-west-2b",
                                "name": "us-west-2b",
                                "uuid": "d115619a-3d1a-4e67-875d-60b99d1b8832",
                                "active": True,
                                "subnet": "subnet-eb77a48d"
                            },
                            {
                                "code": "us-west-2c",
                                "name": "us-west-2c",
                                "uuid": "ee7aa4be-a80e-47d4-8bce-c22869492456",
                                "active": True,
                                "subnet": "subnet-1598fa4e"
                            }
                        ],
                        "active": True,
                        "config": {},
                        "details": {
                            "sg_id": "sg-25c4eb5f"
                        },
                        "ybImage": "ami-b63ae0ce",
                        "latitude": 43.804133,
                        "longitude": -120.554201,
                        "securityGroupId": "sg-25c4eb5f"
                    }
                ],
                "userIntent": {
                    "numNodes": 1,
                    "provider": "850b8e32-e190-4411-ba19-c35d64c0f185",
                    "deviceInfo": {
                        "diskIops": 3000,
                        "numVolumes": 1,
                        "throughput": 125,
                        "volumeSize": 250,
                        "storageType": "GP3",
                        "storageClass": "standard"
                    },
                    "enableIPV6": False,
                    "enableYCQL": True,
                    "enableYSQL": True,
                    "regionList": [
                        "211b14a7-156b-415c-b671-677454d3d5ae"
                    ],
                    "useSystemd": False,
                    "enableYEDIS": False,
                    "useTimeSync": True,
                    "awsArnString": "",
                    "instanceTags": [
                        {
                            "name": "yb_dept",
                            "value": "perf"
                        },
                        {
                            "name": "yb_task",
                            "value": "perf"
                        },
                        {
                            "name": "yb_owner",
                            "value": "sshaikh"
                        }
                    ],
                    "instanceType": "c5.large",
                    "masterGFlags": [],
                    "providerType": "aws",
                    "universeName": "sshaikh-test1",
                    "accessKeyCode": "yb-dev-sshaikh-aws_850b8e32-e190-4411-ba19-c35d64c0f185-key",
                    "tserverGFlags": [],
                    "assignPublicIP": True,
                    "enableYCQLAuth": False,
                    "enableYSQLAuth": False,
                    "replicationFactor": "1",
                    "ybSoftwareVersion": "2.15.1.0-b32",
                    "enableExposingService": "UNEXPOSED",
                    "enableNodeToNodeEncrypt": True,
                    "enableClientToNodeEncrypt": True
                },
                "clusterType": "PRIMARY",
                "placementInfo": {
                    "cloudList": [
                        {
                            "code": "aws",
                            "uuid": "850b8e32-e190-4411-ba19-c35d64c0f185",
                            "regionList": [
                                {
                                    "code": "us-west-2",
                                    "name": "US West (Oregon)",
                                    "uuid": "211b14a7-156b-415c-b671-677454d3d5ae",
                                    "azList": [
                                        {
                                            "name": "us-west-2a",
                                            "uuid": "1bcdf140-e857-457e-a4f3-385eab0d60fe",
                                            "subnet": "subnet-11798e59",
                                            "numNodesInAZ": 1,
                                            "isAffinitized": True,
                                            "replicationFactor": 1
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            }
        ],
        "firstTry": True,
        "capability": "EDITS_ALLOWED",
        "nodePrefix": "yb-dev-sshaikh-test1",
        "universeUUID": "eb957caf-639f-4c63-91ad-65281b69d460",
        "allowInsecure": True,
        "importedState": "NONE",
        "resetAZConfig": False,
        "nodeDetailsSet": [
            {
                "state": "ToBeAdded",
                "azUuid": "1bcdf140-e857-457e-a4f3-385eab0d60fe",
                "nodeIdx": 1,
                "isMaster": False,
                "cloudInfo": {
                    "az": "us-west-2a",
                    "cloud": "aws",
                    "region": "us-west-2",
                    "subnet_id": "subnet-11798e59",
                    "useTimeSync": True,
                    "instance_type": "c5.large",
                    "assignPublicIP": True
                },
                "isTserver": True,
                "cronsActive": True,
                "isYqlServer": True,
                "isYsqlServer": True,
                "isRedisServer": True,
                "masterRpcPort": 7100,
                "placementUuid": "4b3f8a21-03d8-4950-b85f-ef408094596c",
                "ybPrebuiltAmi": False,
                "masterHttpPort": 7000,
                "tserverRpcPort": 9100,
                "tserverHttpPort": 9000,
                "nodeExporterPort": 9300,
                "yqlServerRpcPort": 9042,
                "yqlServerHttpPort": 12000,
                "ysqlServerRpcPort": 5433,
                "redisServerRpcPort": 6379,
                "ysqlServerHttpPort": 13000,
                "redisServerHttpPort": 11000,
                "disksAreMountedByUUID": True
            }
        ],
        "regionsChanged": False,
        "universePaused": False,
        "userAZSelected": False,
        "updateSucceeded": True,
        "backupInProgress": False,
        "clusterOperation": "CREATE",
        "nextClusterIndex": 1,
        "nodeExporterUser": "prometheus",
        "updateInProgress": False,
        "extraDependencies": {
            "installNodeExporter": True
        },
        "remotePackagePath": "",
        "communicationPorts": {
            "masterRpcPort": 7100,
            "masterHttpPort": 7000,
            "tserverRpcPort": 9100,
            "tserverHttpPort": 9000,
            "yqlServerRpcPort": 9042,
            "yqlServerHttpPort": 12000,
            "ysqlServerRpcPort": 5433,
            "redisServerRpcPort": 6379,
            "ysqlServerHttpPort": 13000,
            "redisServerHttpPort": 11000
        },
        "currentClusterType": "PRIMARY",
        "itestS3PackagePath": "",
        "allowGeoPartitioning": False,
        "nodesResizeAvailable": False,
        "sourceXClusterConfigs": [],
        "targetXClusterConfigs": [],
        "encryptionAtRestConfig": {
            "key_op": "UNDEFINED"
        },
        "expectedUniverseVersion": -1,
        "rootAndClientRootCASame": True,
        "setTxnTableWaitCountFlag": False
    }
