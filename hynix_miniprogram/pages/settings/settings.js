const app = getApp()
const api = require('../../utils/api')

Page({
  data: {
    serverUrl: 'https://ought-aurora-shapes-fraser.trycloudflare.com',
    status: ''
  },

  onLoad() {
    const url = app.globalData.serverUrl
    this.setData({ serverUrl: url })
  },

  onUrlInput(e) {
    this.setData({ serverUrl: e.detail.value })
  },

  onSave() {
    const url = this.data.serverUrl.replace(/\/+$/, '')
    app.globalData.serverUrl = url
    wx.setStorageSync('serverUrl', url)
    wx.showToast({ title: '已保存', icon: 'success' })
  },

  onTest() {
    const url = this.data.serverUrl.replace(/\/+$/, '')
    app.globalData.serverUrl = url
    this.setData({ status: '测试中...' })
    api.getRealtimeSignal().then(d => {
      if (d && d.signal) {
        this.setData({ status: '✅ 连接成功！信号: ' + (d.signal.signal || '?') })
      } else {
        this.setData({ status: '⚠️ 服务返回异常数据' })
      }
    }).catch(e => {
      this.setData({ status: '❌ 连接失败: ' + (e.errMsg || '超时') })
    })
  }
})
