#!/bin/bash
# Bootstrap for the GDAL worker.
#
# Runs once at first boot. Subsequent DAG-driven starts reuse the installed Docker
# and whatever image layers are already cached on the root volume — which is why
# the instance is stopped rather than terminated between runs.
set -euxo pipefail

dnf update -y
dnf install -y docker

systemctl enable --now docker
usermod -aG docker ec2-user

# Run Command executes as root, but the layer cache should survive a stop/start,
# so keep Docker's data root on the root volume (the default) rather than on
# instance store.
docker info >/dev/null

# AL2023 ships the SSM agent and AWS CLI v2 preinstalled; make sure the agent is
# actually enabled, since a worker unreachable by SSM is a worker the DAG cannot use.
systemctl enable --now amazon-ssm-agent

echo "gdal worker bootstrap complete"
