const api = require('../../utils/api')

Page({
  data: {
    dailyResults: [],
    dataStart: '--', dataEnd: '--', days: 0,
    idWinRate: '--', idTrades: '--', idAvgRet: '--', idAvgWin: '--',
    idAvgLoss: '--', idCumRet: '--', idSharpe: '--', idDD: '--',
    contempCorr: '--', contempP: '--', lead1Corr: '--', lead1P: '--',
    updateTime: '加载中...'
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
    return api.getBacktest().then(d => {
      if (!d || !d.daily) return
      const daily = d.daily
      const ov = daily.overall || {}

      // Build daily results table across thresholds
      const thresholds = [0.5, 1.0, 1.5, 2.0, 3.0, 5.0]
      const dailyResults = []
      thresholds.forEach(t => {
        const bt = (daily.by_threshold || {})[String(t)] || (daily.by_threshold || {})[t]
        if (bt && bt.trades) {
          dailyResults.push({
            thresh: t,
            trades: bt.trades,
            winRate: bt.win_rate ? (bt.win_rate * 100).toFixed(1) + '%' : '--',
            sharpe: bt.sharpe ? bt.sharpe.toFixed(2) : '--',
            dd: bt.max_drawdown_pct ? bt.max_drawdown_pct.toFixed(1) + '%' : '--',
            cumRet: bt.cum_return_pct ? bt.cum_return_pct.toFixed(1) + '%' : '--',
          })
        }
      })

      const id = d.intraday || {}

      this.setData({
        dailyResults,
        dataStart: ov.data_start || '--',
        dataEnd: ov.data_end || '--',
        days: ov.days || 0,
        idWinRate: id.win_rate ? (id.win_rate * 100).toFixed(1) + '%' : '--',
        idTrades: id.total_trades || '--',
        idAvgRet: id.avg_return ? id.avg_return.toFixed(2) + '%' : '--',
        idAvgWin: id.avg_win ? id.avg_win.toFixed(2) + '%' : '--',
        idAvgLoss: id.avg_loss ? id.avg_loss.toFixed(2) + '%' : '--',
        idCumRet: id.cum_return ? id.cum_return.toFixed(1) + '%' : '--',
        idSharpe: id.sharpe ? id.sharpe.toFixed(2) : '--',
        idDD: id.max_drawdown ? id.max_drawdown.toFixed(1) + '%' : '--',
        contempCorr: ov.contemp_corr !== undefined ? ov.contemp_corr.toFixed(4) : '--',
        contempP: ov.contemp_p !== undefined ? ov.contemp_p.toExponential(2) : '--',
        lead1Corr: ov.lead1_corr !== undefined ? ov.lead1_corr.toFixed(4) : '--',
        lead1P: ov.lead1_p !== undefined ? ov.lead1_p.toExponential(2) : '--',
        updateTime: `更新: ${new Date().toLocaleTimeString()}`
      })
    }).catch(() => {
      this.setData({ updateTime: '⚠️ 服务未连接' })
    })
  }
})
