# fluvio-connectors
The go to place for official fluvio connectors

# Developer notes

To build the official for a minikube k8s cluster you should do:
```bash
eval $(minikube -p minikube docker-env)`
for i in syslog test-connector; do CONNECTOR_NAME=$i make official-containers; done
```
