#!/usr/bin/env bash
set -euo pipefail

REGION=${AWS_REGION:-us-east-1}
KEY_NAME=${KEY_NAME:-kora-bench}
SECURITY_GROUP=${SECURITY_GROUP:-}
AMI_ID=${AMI_ID:-}
SUBNET_ID=${SUBNET_ID:-}

if [ -z "$AMI_ID" ]; then
    echo "Looking up latest Amazon Linux 2023 AMI..."
    AMI_ID=$(aws ec2 describe-images \
        --region "$REGION" \
        --owners amazon \
        --filters "Name=name,Values=al2023-ami-*-x86_64" "Name=state,Values=available" \
        --query 'sort_by(Images, &CreationDate)[-1].ImageId' \
        --output text)
    echo "Using AMI: $AMI_ID"
fi

if [ -z "$SECURITY_GROUP" ]; then
    echo "Creating security group..."
    VPC_ID=$(aws ec2 describe-vpcs --region "$REGION" --filters "Name=is-default,Values=true" --query 'Vpcs[0].VpcId' --output text)
    SECURITY_GROUP=$(aws ec2 create-security-group \
        --region "$REGION" \
        --group-name kora-bench-sg \
        --description "Kora benchmark security group" \
        --vpc-id "$VPC_ID" \
        --query 'GroupId' --output text)
    aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SECURITY_GROUP" \
        --protocol tcp --port 22 --cidr 0.0.0.0/0
    aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SECURITY_GROUP" \
        --protocol tcp --port 6379-6401 --source-group "$SECURITY_GROUP"
    echo "Security group: $SECURITY_GROUP"
fi

if ! aws ec2 describe-key-pairs --region "$REGION" --key-names "$KEY_NAME" &>/dev/null; then
    echo "Creating key pair..."
    aws ec2 create-key-pair --region "$REGION" --key-name "$KEY_NAME" \
        --query 'KeyMaterial' --output text > "${KEY_NAME}.pem"
    chmod 600 "${KEY_NAME}.pem"
    echo "Key saved to ${KEY_NAME}.pem"
fi

launch_instance() {
    local name=$1
    local instance_type=$2

    echo "Launching $name ($instance_type)..."
    local instance_id
    instance_id=$(aws ec2 run-instances \
        --region "$REGION" \
        --image-id "$AMI_ID" \
        --instance-type "$instance_type" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SECURITY_GROUP" \
        ${SUBNET_ID:+--subnet-id "$SUBNET_ID"} \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name}]" \
        --query 'Instances[0].InstanceId' --output text)
    echo "$name instance: $instance_id"

    echo "Waiting for $name to be running..."
    aws ec2 wait instance-running --region "$REGION" --instance-ids "$instance_id"

    local public_ip
    public_ip=$(aws ec2 describe-instances --region "$REGION" \
        --instance-ids "$instance_id" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    local private_ip
    private_ip=$(aws ec2 describe-instances --region "$REGION" \
        --instance-ids "$instance_id" \
        --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
    echo "$name public IP: $public_ip"
    echo "$name private IP: $private_ip"

    eval "${name^^}_INSTANCE_ID=$instance_id"
    eval "${name^^}_PUBLIC_IP=$public_ip"
    eval "${name^^}_PRIVATE_IP=$private_ip"
}

launch_instance "server" "m5.large"
launch_instance "loadgen" "c5n.large"

cat <<EOF

============================================
  AWS Benchmark Instances Launched
============================================
Server (m5.large):  $SERVER_PUBLIC_IP (private: $SERVER_PRIVATE_IP)
LoadGen (c5n.large): $LOADGEN_PUBLIC_IP (private: $LOADGEN_PRIVATE_IP)

Next steps:
  1. SSH into the server:
     ssh -i ${KEY_NAME}.pem ec2-user@${SERVER_PUBLIC_IP}

  2. Copy setup script to server:
     scp -i ${KEY_NAME}.pem aws/provision-server.sh ec2-user@${SERVER_PUBLIC_IP}:~

  3. Copy loadgen script:
     scp -i ${KEY_NAME}.pem aws/provision-loadgen.sh bench-memtier.sh ec2-user@${LOADGEN_PUBLIC_IP}:~

  4. Run provisioning on each instance, then:
     ssh ec2-user@${LOADGEN_PUBLIC_IP}
     TARGET_HOST=${SERVER_PRIVATE_IP} ./bench-memtier.sh

  To terminate when done:
     aws ec2 terminate-instances --instance-ids ${SERVER_INSTANCE_ID} ${LOADGEN_INSTANCE_ID}
EOF
