#!/bin/bash
# SK-XN 联动信号系统 一键启动脚本
# ============================================

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "=========================================="
echo "  SK→XN 实时联动信号系统"
echo "=========================================="
echo ""

# Clean any existing Flask processes
pkill -f "python3 app.py" 2>/dev/null

# Clear proxy
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

# Start Flask
echo "🚀 启动服务器..."
python3 app.py &
sleep 3

IP=$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || echo "127.0.0.1")
PORT="5800"
echo ""
echo "✅ 服务器已启动！"
echo "=========================================="
echo "📱 手机访问 (同一WiFi):"
echo "   实时信号:  http://$IP:$PORT/intraday"
echo "   综合分析:  http://$IP:$PORT/report/v3"
echo "   健康检查:  http://$IP:$PORT/api/health"
echo "=========================================="
echo ""
echo "💡 添加到手机主屏幕获得App体验"
echo "   按 Ctrl+C 停止服务器"
echo "=========================================="

wait
