const api = require('../../utils/api')

Page({
  data: {
    sigClass: 'hold', sigColorClass: 'orange', sigChar: '◆', sigText: '观望', sigSub: '加载中...',
    dot1: '', dot2: '', dot3: '',
    skPrice: '--', skChange: '--', skColor: '', skSub: '--',
    xnPrice: '--', xnChange: '--', xnColor: '', xnSub: '--',
    volRatio: '--', volColor: '',
    mom5: '--', mom5Color: '', mom15: '--', mom15Color: '', mom30: '--', mom30Color: '',
    sigHistory: [],
    updateTime: '加载中...',
    chartData: []
  },

  onLoad() {
    // Try to load from local server, prompt if fails
    this.fetchSignal()
    this.timer = setInterval(() => this.fetchSignal(), 10000)
  },

  onUnload() {
    if (this.timer) clearInterval(this.timer)
  },

  onPullDownRefresh() {
    this.fetchSignal().then(() => wx.stopPullDownRefresh())
  },

  fetchSignal() {
    return api.getRealtimeSignal().then(d => {
      if (!d || d.error) return
      const s = d.signal || {}
      this.updateSignalUI(s)
      this.updatePriceUI(d)
      this.drawChart(s)
    }).catch(() => {
      // Server may not be running
      this.setData({
        sigSub: '⚠️ 请先启动后台服务',
        updateTime: '服务未连接'
      })
    })
  },

  updateSignalUI(s) {
    const dir = s.signal || 'hold'
    const conf = s.confidence || 'low'
    let sigChar, sigColorClass, cls, sigText

    if (dir === 'buy') {
      sigChar = '▲'; sigText = '做多'; sigColorClass = 'green'; cls = 'buy'
    } else if (dir === 'sell') {
      sigChar = '▼'; sigText = '做空'; sigColorClass = 'red'; cls = 'sell'
    } else {
      sigChar = '◆'; sigText = '观望'; sigColorClass = 'orange'; cls = 'hold'
    }

    const n = conf === 'high' ? 3 : conf === 'medium' ? 2 : 1
    const dots = { dot1: n >= 1 ? 'on' : '', dot2: n >= 2 ? 'on' : '', dot3: n >= 3 ? 'on' : '' }

    let sub = ''
    if (s.strength) sub += `强度 ${s.strength}%`
    if (s.sk_last_5min_ret !== undefined) sub += ` 5分: ${s.sk_last_5min_ret > 0 ? '+' : ''}${s.sk_last_5min_ret.toFixed(2)}%`

    // Update signal history
    let hist = this.data.sigHistory
    if (dir !== hist[0]) {
      hist.unshift(dir)
      if (hist.length > 40) hist.pop()
    }

    this.setData({
      sigClass: cls, sigColorClass, sigChar, sigText, sigSub: sub,
      ...dots, sigHistory: hist
    })
  },

  updatePriceUI(d) {
    const s = d.signal || {}
    const xn = d.xn || {}

    // SK
    const skRet = s.sk_last_5min_ret || 0
    this.setData({
      skPrice: (s.sk_latest || 0).toLocaleString(),
      skChange: `${skRet >= 0 ? '+' : ''}${skRet.toFixed(2)}%`,
      skColor: skRet >= 0 ? 'green' : 'red',
      skSub: `30分: ${(s.sk_last_30min_ret || 0).toFixed(2)}%`,
    })

    // XN
    const xnPx = xn.close || 0
    const xnCh = xn.change_pct || 0
    this.setData({
      xnPrice: xnPx.toFixed(2),
      xnChange: `${xnCh >= 0 ? '+' : ''}${xnCh.toFixed(2)}%`,
      xnColor: xnCh >= 0 ? 'green' : 'red',
      xnSub: `开:${(xn.open || 0).toFixed(2)} 高:${(xn.high || 0).toFixed(2)} 低:${(xn.low || 0).toFixed(2)}`,
    })

    // Momentum
    const vr = s.sk_vol_ratio || 1
    this.setData({
      volRatio: `${vr.toFixed(2)}x`,
      volColor: s.sk_vol_surge ? 'green' : '',
      mom5: `${skRet >= 0 ? '+' : ''}${skRet.toFixed(2)}%`,
      mom5Color: skRet > 1 ? 'green' : skRet < -1 ? 'red' : '',
      mom15: `${(s.sk_last_15min_ret || 0) >= 0 ? '+' : ''}${(s.sk_last_15min_ret || 0).toFixed(2)}%`,
      mom15Color: (s.sk_last_15min_ret || 0) > 2 ? 'green' : (s.sk_last_15min_ret || 0) < -2 ? 'red' : '',
      mom30: `${(s.sk_last_30min_ret || 0) >= 0 ? '+' : ''}${(s.sk_last_30min_ret || 0).toFixed(2)}%`,
      mom30Color: (s.sk_last_30min_ret || 0) > 3 ? 'green' : (s.sk_last_30min_ret || 0) < -3 ? 'red' : '',
    })

    this.setData({
      chartData: s.chart || [],
      updateTime: `更新: ${d.server_time || new Date().toLocaleTimeString()}`
    })
  },

  drawChart(s) {
    if (!s || !s.chart || s.chart.length < 2) return
    const data = s.chart
    const closes = data.map(d => d.close)
    const vols = data.map(d => d.volume || 0)

    const query = wx.createSelectorQuery()
    query.select('#priceCanvas').fields({ node: true, size: true }).exec(res => {
      if (!res || !res[0]) return
      const canvas = res[0].node
      const ctx = canvas.getContext('2d')
      const dpr = wx.getSystemInfoSync().pixelRatio
      const w = res[0].width, h = res[0].height
      canvas.width = w * dpr
      canvas.height = h * dpr
      ctx.scale(dpr, dpr)
      ctx.clearRect(0, 0, w, h)

      const pad = { top: 20, bottom: 20, left: 10, right: 10 }
      const cw = w - pad.left - pad.right
      const ch = h - pad.top - pad.bottom
      const min = Math.min(...closes), max = Math.max(...closes)
      const range = max - min || 1

      // Price line
      ctx.beginPath()
      closes.forEach((c, i) => {
        const x = pad.left + (i / (closes.length - 1)) * cw
        const y = pad.top + (1 - (c - min) / range) * ch
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y)
      })
      ctx.strokeStyle = '#3b82f6'
      ctx.lineWidth = 2
      ctx.stroke()

      // Volume bars
      const avgVol = vols.reduce((a, b) => a + b, 0) / vols.length || 1
      const barW = cw / vols.length * 0.6
      vols.forEach((v, i) => {
        const x = pad.left + (i / vols.length) * cw + cw / vols.length * 0.2
        const bh = (v / Math.max(...vols)) * ch * 0.4
        ctx.fillStyle = v > avgVol * 1.5 ? '#22c55e' : v > avgVol ? '#f59e0b' : '#475569'
        ctx.fillRect(x, h - pad.bottom - bh, barW, bh)
      })
    })
  }
})
