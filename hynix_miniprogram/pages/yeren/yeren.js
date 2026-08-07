const api = require('../../utils/api')

Page({
  data: {
    n: '--', bull: '--', bear: '--', neutral: '--', avg: '--',
    lastDate: '--', lastTitle: '待抓取', lastDir: '中性', lastSent: '0.00',
    indices: [], btRate: '--', btN: '--', btBull: '--', btBear: '--',
    btRows: [], articles: [],
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
    return api.getYeren().catch(() => null).then(d => {
      if (!d || !d.ok) {
        this.setData({ updateTime: '⚠️ 服务未连接' })
        return
      }
      const st = d.stats || {}
      const last = d.last || {}
      const bt = d.backtest || {}
      const rows = (bt.rows || []).slice().reverse()
      const bull = rows.filter(r => r.pred === '涨')
      const bear = rows.filter(r => r.pred === '跌')
      this.setData({
        n: st.n, bull: st.bull, bear: st.bear, neutral: st.neutral,
        avg: st.avg_score !== null && st.avg_score !== undefined ? (st.avg_score >= 0 ? '+' : '') + st.avg_score : '--',
        lastDate: last.date || '--',
        lastTitle: last.title || '待抓取',
        lastDir: last.direction || '中性',
        lastSent: last.sentiment !== null && last.sentiment !== undefined ? ((last.sentiment >= 0 ? '+' : '') + last.sentiment.toFixed(2)) : '--',
        indices: Object.entries(d.index_compare || {}).map(([name, v]) => ({
          name,
          corrSame: v.corr_same === null ? '--' : v.corr_same.toFixed(3),
          corrNext: v.corr_next === null ? '--' : v.corr_next.toFixed(3),
          hitRate: v.hit_rate === null ? '--' : v.hit_rate + '%',
          bullRate: v.bull_rate === null ? '--' : v.bull_rate + '%'
        })),
        btRate: bt.rate === null || bt.rate === undefined ? '--' : bt.rate + '%',
        btN: bt.n, btBull: bull.length ? bull.filter(r => r.hit).length + '/' + bull.length : '--',
        btBear: bear.length ? bear.filter(r => r.hit).length + '/' + bear.length : '--',
        btRows: rows.map(r => ({
          date: r.date, pred: r.pred, next: r.next,
          pct: (r.sz_pct >= 0 ? '+' : '') + r.sz_pct + '%',
          hit: r.hit
        })),
        articles: (d.recent || []).map(r => ({
          date: r.date,
          title: (r.title || '').replace(/_野人哥_淘股吧$/, ''),
          dir: r.direction || '中性'
        })),
        updateTime: `更新: ${new Date().toLocaleTimeString()}`
      })
    })
  }
})
