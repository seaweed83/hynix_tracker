---
name: yeren-analysis
description: 野人哥(淘股吧/抖音全网同名, 情绪量化ACB交易系统博主)情绪挖掘、打标、与A股四指数关联度分析及回测。当用户提到"野人哥""yeren""情绪量化""ACB""淘股吧野人哥"或需要分析其作品情绪/大盘预测力时使用。包含抓取、打分、关联度回测、面板复用四步完整工作流。
---

# 野人哥情绪量化分析

对野人哥（淘股吧博客 + 抖音，情绪量化ACB交易系统）历史作品做全量情绪打标，
并与 A 股四大指数（上证/深成/创业板/科创50）做关联度与预判回测分析。

## 关键事实（分析前提，不要重新研究）

- 用户ID `12843035`；列表AJAX接口 `https://www.taoguba.com.cn/mBlogTopicAjax?userID=12843035&sortFlag=W&pageNo=N`
- 详情URL `https://www.taoguba.com.cn/a/{newTopicID}`；作品日期用详情页内日期
- 全量样本：21页201条 → 去重 **179篇**（2025-10-12 ~ 2026-08-05）
- 已得结论（勿推翻）：
  - 当日相关：创业板0.144 > 深成0.127 > 科创50 0.124 > 上证0.052（弱相关）
  - 方向命中：科创50 59.7% 最高；全样本 56.7% vs 基准55.8%（无优势）
  - 次日相关全部为负（约-0.10），**对大盘无预测力**
  - 29条人工预判回测：次日收盘命中 **34.5%**（基准≈54%）→ 明确低于随机
  - 价值在**个股情绪量化**，大盘预判仅用于仓位管理

## 文件位置

- 数据库 `yeren.db`（路径可用 `YEREN_DB` 覆盖，默认 `~/Documents/trae_projects/yeren_analysis/yeren.db`）
  - 表 `yeren_daily`：date/title/sentiment/direction（看多/看空/中性）
  - 表 `market_daily`：date + `sh_pct/sz_pct/cyb_pct/kc_pct`（上证/深成/创业板/科创50）
- 爬虫 `yeren_analysis/scraper.py`：`fetch_all_pages` / `scrape_all` / `score_text` / `correlation`
- 面板：`hynix_tracker/app.py` 的 `/api/yeren`（完整API）+ `templates/yeren.html`（独立分析页）
- 报告：`yeren_analysis/野人哥交易体系学习笔记.md`、`野人哥-大盘关联度报告.md`、`野人哥大盘预判回测报告.md`

## 工作流

### 1. 抓取
```bash
cd ~/Documents/trae_projects/yeren_analysis
# 抓取前必须清代理环境变量(akshare/淘股吧均可能受影响)
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy
python3 scraper.py scrape_all
```
`scrape_all` 用 `fetch_all_pages` 翻21页拉全量，详情入库 `yeren_daily`。

### 2. 打分
词典计数法：36个多头词 / 30个空头词，`score_text` 输出 sentiment 分 + direction。
当日新增作品在抓取时自动打标。手动补打：
```bash
python3 scraper.py score  # 若有该命令
```

### 3. 关联度 + 回测
- 关联度：情绪分与四指数当日/次日涨跌幅 Pearson 相关（`correlation`）
- 预判回测：**必须人工从详情页提取明确"看涨/看跌"句子**（避免词典自动判向噪音），
  对深成指 `sz_pct` 次日方向验证；基准≈54%（深成指次日上涨占比）
- 结论写入 markdown 报告

### 4. 面板复用
- 更新数据后重启 `hynix_tracker`（`PORT` 默认5800，被占时用5801）
- `/api/yeren` 返回：stats / last / corr / index_compare(四指数) / backtest(29条) / recent(30篇)
- 页面 `/yeren`（完整分析）+ `/intraday`（情绪面板挂件）
- 验证：`curl -s localhost:PORT/api/yeren | python3 -m json.tool`；页面 `curl -s -o /dev/null -w "%{http_code}" localhost:PORT/yeren`

## 注意
- 抓取用 curl/requests 前先 `unset` 代理变量；akshare 抓指数同理
- 情绪打标是"词典计数"不是模型，勿向用户过度承诺预测力
- 四指数列：`market_daily` 的 `sh_pct`(索引2) `sz_pct`(7) `cyb_pct`(4) `kc_pct`(8)
