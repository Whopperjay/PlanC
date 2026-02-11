#!/bin/bash

# Configuration
SERVER_USER="root"
SERVER_IP="192.168.1.43"
REMOTE_DIR="/opt/planc-databackend"

echo "=== Plan C Data Backend Deployment ==="
echo "Target: $SERVER_USER@$SERVER_IP"
echo "Remote Dir: $REMOTE_DIR"
echo "======================================"

# Step 1: Check SSH Access and Key
echo "Checking SSH access and Git credentials on server..."

# Check if we can SSH at all
ssh -q -o BatchMode=yes -o ConnectTimeout=5 $SERVER_USER@$SERVER_IP exit
if [ $? -ne 0 ]; then
    echo "Error: Cannot connect to $SERVER_USER@$SERVER_IP via SSH."
    echo "Please ensure you have SSH access configured (ssh-copy-id recommended)."
    exit 1
fi

# Check for SSH key on the server
HAS_KEY=$(ssh $SERVER_USER@$SERVER_IP "[ -f ~/.ssh/id_ed25519.pub ] || [ -f ~/.ssh/id_rsa.pub ] && echo 'yes' || echo 'no'")

if [ "$HAS_KEY" == "no" ]; then
    echo "No SSH key found on server. Generating one now..."
    ssh $SERVER_USER@$SERVER_IP "ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ''"
    
    echo ""
    echo "IMPORTANT: You need to add this key to your GitHub repository as a Deploy Key."
    echo "-------------------------------------------------------------------------------"
    ssh $SERVER_USER@$SERVER_IP "cat ~/.ssh/id_ed25519.pub"
    echo "-------------------------------------------------------------------------------"
    echo "1. Copy the key above."
    echo "2. Go to: https://github.com/jerome/PlanC-Databackend/settings/keys/new"
    echo "3. Title: 'F1 Data Server'"
    echo "4. Key: Paste the key"
    echo "5. Check 'Allow write access' (This is CRITICAL)"
    echo "6. Click 'Add key'"
    echo ""
    read -p "Press Enter once you have added the key to GitHub..."
else
    echo "SSH key exists on server. Assuming it's already added to GitHub."
    echo "If git push fails, check that this key is authorized:"
    ssh $SERVER_USER@$SERVER_IP "cat ~/.ssh/id_ed25519.pub 2>/dev/null || cat ~/.ssh/id_rsa.pub 2>/dev/null"
fi

# Step 2: Prepare Remote Directory
echo "Preparing remote directory..."
ssh $SERVER_USER@$SERVER_IP "mkdir -p $REMOTE_DIR"

# Step 3: Copy Files
echo "Copying project files..."
scp -r scripts Dockerfile docker-compose.yml requirements.txt .git $SERVER_USER@$SERVER_IP:$REMOTE_DIR/

# Step 4: Deploy with Docker Compose
echo "Deploying container..."
ssh $SERVER_USER@$SERVER_IP "cd $REMOTE_DIR && docker compose up -d --build"

echo "=== Deployment Complete ==="
echo "You can check logs with: ssh $SERVER_USER@$SERVER_IP 'docker logs -f f1-data-updater'"
