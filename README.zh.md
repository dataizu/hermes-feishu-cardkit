# hermes-feishu-cardkit 中文说明

给 Hermes Agent 飞书通道实装 **CardKit 流式卡片**体验（对标 OpenClaw 官方飞书插件 / ZCode 内置实现）。

## 效果

飞书里的回复不再是一段段文本编辑，而是一张实时卡片：

- 🌊 **打字机流式** —— 回复逐字流入卡片
- 🛠️ **工具折叠面板** —— 工具调用过程收进灰色“工具摘要（N）”面板，默认折叠、点击展开，正文干净
- ⏱️ **状态 footer** —— 完成后底部显示 `已完成 · 耗时 8.4s · 模型名`
- 📐 **排版优化** —— 标题自动降级（H1→H4）、表格/代码块自动加间距，长文在卡片里也好看
- 🔒 **故障兜底** —— 任何 CardKit API 失败自动退回内置文本流式，回复永不丢

## 原理

以同名平台注册**覆盖**内置 `feishu` 通道（注册表 last-writer-wins），适配器继承内置
FeishuAdapter 的全部能力（WebSocket、话题、媒体、审批、配对等），只覆写发送/编辑链路接入
CardKit。现有 `FEISHU_APP_ID`/`FEISHU_APP_SECRET`、白名单、home channel 等配置全部沿用。

```
流式首帧 ──► 建卡实体 ──► 卡片消息（引用 card_id）
   │
文本增量 ──► cardElement.content（累积推送，1s 节流，打字机 diff）
   │
定稿 ──► 整卡替换（折叠工具面板 + footer）──► 关闭流式模式
```

工具边界采用"封卡续新"（seal-and-continue）：间隙时封存当前卡，下一段自动开新卡。

## 安装

```bash
hermes plugins install <owner>/hermes-feishu-cardkit
hermes plugins enable hermes-feishu-cardkit
hermes gateway restart
```

前提：内置飞书通道已配好（FEISHU_APP_ID/SECRET）；飞书应用具备 CardKit 权限
（开发者后台 → 权限管理，多数通过 `hermes gateway setup` 建的应用已有）。

## 配置（均可选）

| 环境变量 | 默认 | 作用 |
|---|---|---|
| `FEISHU_CARDKIT_STREAMING` | 开 | `0`/`false` 整体关闭卡片，退回文本流式 |
| `FEISHU_CARDKIT_FOOTER` | 开 | `0`/`false` 隐藏底部状态行 |

## 关于工具面板补丁

`patches/run_turn_runner.tool_progress.patch` 给 `gateway/run_turn_runner.py` 加一行
`tool_progress` 元数据标记，使工具进度行可折叠进卡片。不打此补丁：流式+footer 正常，
仅工具面板为空。

## 许可

MIT
