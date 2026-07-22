const app = getApp()

function getBaseUrl() {
  return app.globalData.serverUrl
}

function request(path) {
  return new Promise((resolve, reject) => {
    wx.request({
      url: getBaseUrl() + path,
      timeout: 10000,
      success: res => {
        if (res.statusCode === 200) resolve(res.data)
        else reject({ errMsg: `HTTP ${res.statusCode}` })
      },
      fail: reject
    })
  })
}

module.exports = {
  getRealtimeSignal: () => request('/api/realtime/signal'),
  getBacktest: () => request('/api/backtest'),
  getReportData: () => request('/api/report/data'),
  getXnData: () => request('/api/xn'),
  getHynixData: () => request('/api/hynix'),
  setServerUrl: url => { app.globalData.serverUrl = url }
}
