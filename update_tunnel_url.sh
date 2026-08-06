#!/bin/bash
# 隧道URL变更自动更新小程序 + 推送
# 用法: ./update_tunnel_url.sh   (无参数, 幂等: URL没变则跳过)
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

LOG="${TUNNEL_LOG:-/private/tmp/cloudflared.log}"

NEW=$(grep -oE 'https://[a-z0-9.-]+\.trycloudflare\.com' "$LOG" | tail -1)
if [ -z "$NEW" ]; then
  echo "错误: 隧道日志中未找到URL ($LOG)"; exit 1
fi
OLD=$(grep -oE 'https://[a-z0-9.-]+\.trycloudflare\.com' hynix_miniprogram/app.js | head -1)
if [ "$NEW" = "$OLD" ]; then
  echo "URL未变化: $NEW"; exit 0
fi

echo "URL变更: $OLD -> $NEW"
sed -i '' "s|https://[a-z0-9.-]*\.trycloudflare\.com|$NEW|g" \
  hynix_miniprogram/app.js hynix_miniprogram/pages/settings/settings.js

git add hynix_miniprogram/app.js hynix_miniprogram/pages/settings/settings.js
git commit -m "chore: 更新小程序serverUrl -> $NEW (隧道重启)" -q
git push origin main -q
echo "已更新小程序URL并推送: $NEW"
