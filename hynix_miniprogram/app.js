App({
  globalData: {
    serverUrl: 'https://ought-aurora-shapes-fraser.trycloudflare.com',
    signalData: null,
    backtestData: null,
    refreshTimer: null
  },
  onLaunch() {
    const saved = wx.getStorageSync('serverUrl')
    if (saved) this.globalData.serverUrl = saved
    console.log('SK-XN 联动信号启动, server:', this.globalData.serverUrl)
  }
})
