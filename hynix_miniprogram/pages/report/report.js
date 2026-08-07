const api = require('../../utils/api')

Page({
  data: {
    xnPrice: '--', correlation: '--', intradayR: '--', signalScore: '--',
    btWinRate: '--', btTrades: '--', btSharpe: '--', btDD: '--',
    idWinRate: '--', idTrades: '--', idAvgRet: '--', idCumRet: '--',
    updateTime: '加载中...'
  },

  goYeren() {
    wx.navigateTo({ url: '/pages/yeren/yeren' })
  },

  onLoad() {
    this.fetchData()
  },

  onShow() {
    this.fetchData()
  },

  onPullDownRefresh() {
    this.fetchData().then(() => wx.stopPullDownRefresh())
  },

  fetchData() {
    return Promise.all([
      api.getReportData().catch(() => null),
      api.getBacktest().catch(() => null)
    ]).then(([report, backtest]) => {
      if (report && report.xn) {
        const s = report.xn.stats || {}
        const sig = report.signal || {}
        this.setData({
          xnPrice: (report.realtime?.close || s.close || '--').toString(),
          correlation: s.correlation ? `r=${s.correlation.toFixed(3)}` : '--',
          intradayR: s.rolling_corr_last ? s.rolling_corr_last.toFixed(3) : '--',
          signalScore: sig.score !== undefined ? sig.score.toString() : '--',
        })
      }
      if (backtest && backtest.daily) {
        const bt2 = (backtest.daily.by_threshold || {})['2.0'] || {}
        const id = backtest.intraday || {}
        this.setData({
          btWinRate: bt2.win_rate ? (bt2.win_rate * 100).toFixed(1) + '%' : '--',
          btTrades: bt2.trades || '--',
          btSharpe: bt2.sharpe ? bt2.sharpe.toFixed(2) : '--',
          btDD: bt2.max_drawdown_pct ? bt2.max_drawdown_pct.toFixed(1) + '%' : '--',
          idWinRate: id.win_rate ? (id.win_rate * 100).toFixed(1) + '%' : '--',
          idTrades: id.total_trades || '--',
          idAvgRet: id.avg_return ? id.avg_return.toFixed(2) + '%' : '--',
          idCumRet: id.cum_return ? id.cum_return.toFixed(1) + '%' : '--',
        })
      }
      this.setData({ updateTime: `更新: ${new Date().toLocaleTimeString()}` })
    }).catch(() => {
      this.setData({ updateTime: '⚠️ 服务未连接' })
    })
  }
})
