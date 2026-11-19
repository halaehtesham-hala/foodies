#!/bin/bash
# One-time server setup. Paste this whole file into:
#   EC2 > Launch instance > Advanced details > User data
# It installs Python, creates the Foodies service, and installs the deploy script.
set -euxo pipefail

dnf install -y python3 python3-pip unzip
mkdir -p /opt/foodies/releases /var/lib/foodies
chown -R ec2-user:ec2-user /opt/foodies /var/lib/foodies

# ---- deploy script: downloads the latest build from S3 and switches to it ----
cat > /usr/local/bin/foodies-deploy.sh <<'DEPLOY'
#!/bin/bash
set -euo pipefail
BUCKET="$1"
APP=/opt/foodies

TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
export AWS_DEFAULT_REGION=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/region)

TMP=$(mktemp -d)
echo "Downloading s3://$BUCKET/builds/latest.zip"
aws s3 cp "s3://$BUCKET/builds/latest.zip" "$TMP/build.zip" --only-show-errors

REL="$APP/releases/$(date +%Y%m%d%H%M%S)"
mkdir -p "$REL"
unzip -q "$TMP/build.zip" -d "$REL"
echo "Unpacked build:"; cat "$REL/build-info.txt"

[ -d "$APP/venv" ] || python3 -m venv "$APP/venv"
"$APP/venv/bin/pip" install -q -r "$REL/requirements.txt"
chown -R ec2-user:ec2-user "$REL" "$APP/venv"

ln -sfn "$REL" "$APP/current"
systemctl restart foodies

for i in $(seq 1 15); do
  if curl -fsS http://127.0.0.1/health; then
    echo
    echo "Deployed version $(cat "$REL/VERSION")"
    break
  fi
  if [ "$i" -eq 15 ]; then echo "Health check failed"; exit 1; fi
  sleep 2
done

# keep only the 3 newest releases
ls -1dt "$APP"/releases/* | tail -n +4 | xargs -r rm -rf
DEPLOY
chmod +x /usr/local/bin/foodies-deploy.sh

# ---- systemd service: runs the app with gunicorn on port 80 ----
# One worker on purpose: the demo keeps its menu cache and event bus in memory.
cat > /etc/systemd/system/foodies.service <<'SERVICE'
[Unit]
Description=Foodies web application
After=network.target

[Service]
User=ec2-user
WorkingDirectory=/opt/foodies/current
Environment=FOODIES_DB=/var/lib/foodies/foodies.db
Environment=FOODIES_SECRET=change-this-to-a-long-random-text
Environment=FOODIES_STEP_DELAY=5
ExecStart=/opt/foodies/venv/bin/gunicorn --workers 1 --threads 8 --bind 0.0.0.0:80 app:app
AmbientCapabilities=CAP_NET_BIND_SERVICE
Restart=always

[Install]
WantedBy=multi-user.target
SERVICE
systemctl daemon-reload
systemctl enable foodies   # starts after the first deployment, and on every reboot
