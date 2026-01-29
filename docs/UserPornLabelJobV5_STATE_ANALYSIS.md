# UserPornLabelJobV5 状态存储与转换逻辑分析

## 一、状态存储架构

### 1.1 Redis Key 设计（拆分存储）

V5 采用**多 Key 拆分存储**策略，便于线上系统按需查询：

| Redis Key | 说明 | 值格式 | TTL |
|-----------|------|--------|-----|
| `rec_post:{uid}:porn_user_type_v5` | 用户类型 | `zone_user` / `no_zone_user` / `block_zone_user` / `unknown` | 30天 |
| `rec_post:{uid}:porn_stage_v5` | 试探/冷却阶段 | `probing_mid` / `probing_high` / `stable` / `cooling` | 30天 |
| `rec_post:{uid}:porn_dist_v5` | 分发模式（冗余） | `mid2_high0` / `mid1_high1` / `mid0_high2` / `mid1_high0` | 30天 |
| `rec_post:{uid}:porn_probe_count_v5` | 试探轮次 | 整数（字符串） | 30天 |
| `rec_post:{uid}:porn_cooldown_stage_v5` | 冷却档位 | `0` / `1` / `2` / `3` | 30天 |
| `rec_post:{uid}:porn_cooldown_end_ts_v5` | 冷却结束时间戳 | epoch 毫秒（字符串） | 30天 |
| `rec_post:{uid}:porn_last_feedback_v5` | 最近反馈类型 | `positive` / `negative` / `none` | 30天 |
| `rec_post:{uid}:porn_last_feedback_ts_v5` | 最近反馈时间 | epoch 毫秒（字符串） | 30天 |
| `rec_post:{uid}:porn_last_pos_tag_v5` | 最近正反馈标签 | `mid` / `high` / `explicit` / `unk` / `clean` | 30天 |
| `rec_post:{uid}:porn_last_neg_tag_v5` | 最近负反馈标签 | `mid` / `high` / `explicit` / `unk` / `clean` | 30天 |
| `rec_post:{uid}:porn_last_update_ts_v5` | 最近更新时间 | epoch 毫秒（字符串） | 30天 |
| `rec_post:{uid}:porn_state_flow_v5` | **Flow 汇总 Key** | 完整状态字符串（见下文） | 30天 |

### 1.2 Flow 汇总 Key 格式

**Key**: `rec_post:{uid}:porn_state_flow_v5`

**Value 格式**（分号分隔的键值对）：
```
v5;user_type=no_zone_user;stage=probing_mid;dist=mid2_high0;probe_round=1;
cooling_stage=0;cooling_end_ts=0;
last_feedback=positive;last_feedback_ts=1672444800000;
last_pos_tag=mid;last_neg_tag=unk;last_update_ts=1672444800000
```

**优势**：
- 线上系统只需读取一个 Key 即可获取完整状态
- 便于日志排查和状态回溯
- 字段名直观，可直接定位到流程图中的节点

### 1.3 分发模式（dist）映射

| Stage | dist 值 | 语义 | 对应流程图节点 |
|-------|---------|------|----------------|
| `PROBING_MID` | `mid2_high0` | 最多 2 个 mid | P1/P2（Mid 试探） |
| `PROBING_HIGH` | `mid1_high1` | 最多 1 个 high + 1 个 mid | P3（High 试探） |
| `STABLE` | `mid0_high2` | 最多 2 个 high | P4（稳定期） |
| `COOLING` | `mid1_high0` | 冷却期，仅 1 个 mid | C1/C2/C3（冷却期） |

---

## 二、状态转换逻辑（状态机）

### 2.1 状态定义

```java
public enum StageV5 {
    PROBING_MID,   // 试探：从Mid开始（P1/P2）
    PROBING_HIGH,  // 试探：High阶段（P3）
    STABLE,        // 稳定：已有明确偏好（P4）
    COOLING        // 冷却：等待冷却结束再试探（C1/C2/C3）
}
```

### 2.2 核心转换规则

#### 规则 1：有专区用户（ZONE_USER）
- **强制固定状态**：无论正/负反馈，始终维护为 `STABLE`
- **冷却相关**：`cooldown_stage=0`, `cooldown_end_ts=0`, `probe_count=0`
- **说明**：线上系统可根据 `user_type=zone_user` 自行实现分发限制

#### 规则 2：无专区用户 / 关闭专区用户（NO_ZONE_USER / BLOCK_ZONE_USER）

**状态转换图**：
```
[初始] → PROBING_MID
    ↓
    ├─ 正反馈 → PROBING_HIGH
    │           ├─ 正反馈 → STABLE (probe_count=0, 清空冷却)
    │           └─ 负反馈 → COOLING (stage=2, probe_count++, 冷却7d/3d)
    │
    └─ 负反馈 → COOLING (stage=1, probe_count++, 冷却3d/1d)
                ↓
                [冷却到期] → PROBING_MID (自动回切)
```

**详细转换表**：

| 当前阶段 | 反馈类型 | 下一阶段 | probe_count | cooldown_stage | 冷却天数 |
|---------|---------|---------|-------------|----------------|----------|
| `PROBING_MID` | `POSITIVE` | `PROBING_HIGH` | 不变 | 0 | - |
| `PROBING_MID` | `NEGATIVE` | `COOLING` | **+1** | 1 | no_zone: 3d<br>block: 1d |
| `PROBING_HIGH` | `POSITIVE` | `STABLE` | **清零** | 0 | - |
| `PROBING_HIGH` | `NEGATIVE` | `COOLING` | **+1** | 2 | no_zone: 7d<br>block: 3d |
| `STABLE` | `POSITIVE` | `STABLE` | 不变 | 0 | - |
| `STABLE` | `NEGATIVE` | `COOLING` | **不变** | **3** | no_zone: 14d<br>block: 7d |
| `COOLING` | 任意 | `COOLING` | 不变 | 不变 | 不变 |

**关键设计点**：
1. **B 模式**：只有“试探阶段进入冷却”才递增 `probe_count`；`STABLE → COOLING` 不递增
2. **STABLE → COOLING**：强制 `cooldown_stage=3`（最长冷却），不受历史 `probe_count` 影响
3. **冷却到期自动回切**：当 `nowMs >= cooldown_end_ts` 时，下次有事件触发时自动切回 `PROBING_MID`

### 2.3 冷却期计算逻辑

#### 冷却天数映射

| cooldown_stage | no_zone_user | block_zone_user |
|----------------|--------------|-----------------|
| 1 | 3 天 | 1 天 |
| 2 | 7 天 | 3 天 |
| 3 | 14 天 | 7 天 |

#### 冷却结束时间对齐规则

**关键函数**：`alignToNextIndia20(long tsMs)`

**逻辑**：
1. 计算原始冷却结束时间：`rawEnd = nowMs + days * 24 * 3600 * 1000`
2. 转换为印度时区：`ZonedDateTime zdt = Instant.ofEpochMilli(rawEnd).atZone(INDIA_ZONE)`
3. 对齐到“印度时间 20:00 的下一次到达点”：
   - 如果 `rawEnd` 对应的印度时间 < 20:00：对齐到**当天 20:00**
   - 否则：对齐到**次日 20:00**

**示例**：
- 用户在北京时间 2024-01-15 10:00 进入冷却（3天）
- 原始结束时间：2024-01-18 10:00（北京时间）
- 转换为印度时间：2024-01-18 07:30（印度比北京晚 2.5 小时）
- 对齐后：2024-01-18 20:00（印度时间）= 2024-01-18 22:30（北京时间）

**目的**：确保冷却结束后，用户可以在“印度时间 20:00 后”重新进入试探流程

---

## 三、反馈判定逻辑（复用 V3）

### 3.1 正反馈判定

**条件**（任一满足即可）：
1. **长播**：该标签下所有 postId 中 `maxStandingTime >= 10秒`
2. **明确正反馈行为**：`positiveCount > 0`（点赞、评论、分享、收藏、关注等）

**额外要求**：
- 该标签下 `negativeCount <= 0`（无负反馈）
- 标签不为 `unk` 或 `clean`

**标签优先级**：`explicit > high > mid`（取最高等级）

### 3.2 负反馈判定

**条件**（任一满足即可）：
1. **短播累积**：该标签下 `shortPlayPostCount >= 5`（standingTime < 3秒）
2. **明确负反馈**：`dislikeCount >= 1`（dislike / 不感兴趣）

**标签优先级**：`explicit < high < mid`（取最低等级，更保守）

### 3.3 反馈冲突处理

**规则**：**负反馈优先**（更保守策略）
- 如果同时检测到正反馈和负反馈，判定为 `NEGATIVE`
- 避免“同时有正/负”时过于激进升级

### 3.4 无反馈处理

**规则**：`feedback == NONE` 时，**不写入 Redis**（避免写放大）
- 只有明确的正/负反馈信号才会触发状态更新

---

## 四、用户类型判定（可插拔接口）

### 4.1 判定优先级

1. **优先读取**：`rec_post:{uid}:porn_user_type_v5`（本作业维护的 key）
2. **降级查询**：`rec_post:{uid}:porn_user_type_source`（线上系统/其他作业写入）
3. **默认值**：`UNKNOWN`（可后续替换为 MySQL 查询）

### 4.2 用户类型定义

| 类型 | 说明 | 状态机行为 |
|------|------|-----------|
| `ZONE_USER` | 有专区用户 | 强制 `STABLE`，不进入冷却 |
| `NO_ZONE_USER` | 历史无创建过专区用户 | 正常试探流程，冷却 3/7/14 天 |
| `BLOCK_ZONE_USER` | 关闭专区用户 | 正常试探流程，冷却 1/3/7 天 |
| `UNKNOWN` | 未判定 | 默认走 `NO_ZONE_USER` 流程 |

---

## 五、状态更新时机

### 5.1 触发条件

**必须满足**：
1. 用户有观看事件（event_type=8）
2. 最近 N 条记录中至少包含 2 个 postId（`CAL_NUM=2`）
3. 检测到明确的**正反馈**或**负反馈**信号

**不触发**：
- `feedback == NONE`：不写入 Redis，避免写放大

### 5.2 更新流程

```
1. 读取当前状态（user_type, stage, probe_count, cooldown_stage, cooldown_end_ts）
   ↓
2. 检查冷却是否到期（nowMs >= cooldown_end_ts）
   - 如果到期：自动切回 PROBING_MID，清空冷却信息
   ↓
3. 执行状态转换（transition）
   - 根据当前 stage + feedback 决定下一阶段
   - 计算新的 probe_count、cooldown_stage、cooldown_end_ts
   ↓
4. 批量写入 Redis（所有相关 key 同步更新）
   - 单字段 key（便于按需查询）
   - Flow 汇总 key（完整状态）
```

---

## 六、关键设计亮点

### 6.1 TTL 策略

**问题**：Flink RedisSink 只支持“全流固定 TTL”，无法逐条动态 TTL

**解决方案**：
- TTL 统一设置为 **30 天**（足够覆盖最长冷却 14 天）
- **冷却结束逻辑**通过 `cooldown_end_ts` 时间戳精确控制
- 线上系统判断：`nowMs < cooldown_end_ts` → 冷却中；`nowMs >= cooldown_end_ts` → 可重新试探

### 6.2 状态持久化

**策略**：
- **多 Key 拆分**：便于线上按需查询（如只读 `porn_dist_v5` 做分发）
- **Flow 汇总 Key**：便于排查和完整状态获取
- **同步更新**：所有 key 在同一批次写入，保证一致性

### 6.3 冷却期精确控制

**关键点**：
1. **对齐印度 20:00**：确保冷却结束后满足“20:00 后可重新试探”规则
2. **递增冷却**：根据试探轮次递增冷却天数（1/3/7 或 3/7/14）
3. **STABLE → COOLING**：固定最长冷却，不受历史试探次数影响

### 6.4 写放大控制

**优化**：
- 只有明确反馈信号才写入 Redis
- `feedback == NONE` 时直接返回空结果，不触发写入

---

## 七、线上系统使用指南

### 7.1 快速查询（推荐）

**只读 Flow Key**：
```redis
GET rec_post:{uid}:porn_state_flow_v5
```

解析 value 即可获取：
- `user_type`：用户类型
- `stage`：当前阶段
- `dist`：分发模式（最多几个 mid/high）
- `cooling_end_ts`：冷却结束时间

### 7.2 按需查询

**只读分发模式**：
```redis
GET rec_post:{uid}:porn_dist_v5
```

**判断是否在冷却期**：
```redis
GET rec_post:{uid}:porn_cooldown_end_ts_v5
# 判断：nowMs < cooldown_end_ts → 冷却中
```

### 7.3 分发策略建议

| stage | dist | 分发建议 |
|-------|------|----------|
| `probing_mid` | `mid2_high0` | 最多 2 个 mid 级别内容 |
| `probing_high` | `mid1_high1` | 最多 1 个 high + 1 个 mid |
| `stable` | `mid0_high2` | 最多 2 个 high（强偏好） |
| `cooling` | `mid1_high0` | 冷却期，仅 1 个 mid（保守唤醒） |

**特殊处理**：
- `user_type=zone_user`：线上系统自行实现“不出现 High/Porn，最多 2 个 Mid”的限制

---

## 八、状态转换示例

### 示例 1：无专区用户完整流程

```
初始状态：PROBING_MID, probe_count=0
  ↓ 正反馈（长播10s+）
PROBING_HIGH, probe_count=0
  ↓ 正反馈（长播10s+）
STABLE, probe_count=0, cooldown_stage=0
  ↓ 负反馈（短播>=5）
COOLING, probe_count=0, cooldown_stage=3, 冷却14天
  ↓ 冷却到期（nowMs >= cooldown_end_ts）
PROBING_MID, probe_count=0, cooldown_stage=0
```

### 示例 2：关闭专区用户试探失败

```
初始状态：PROBING_MID, probe_count=0
  ↓ 负反馈（短播>=5）
COOLING, probe_count=1, cooldown_stage=1, 冷却1天
  ↓ 冷却到期
PROBING_MID, probe_count=1, cooldown_stage=0
  ↓ 正反馈
PROBING_HIGH, probe_count=1
  ↓ 负反馈
COOLING, probe_count=2, cooldown_stage=2, 冷却3天
  ↓ 冷却到期
PROBING_MID, probe_count=2, cooldown_stage=0
  ↓ 负反馈
COOLING, probe_count=3, cooldown_stage=3, 冷却7天
```

---

## 九、总结

### 9.1 核心设计原则

1. **只维护状态，不做分发**：V5 作业只负责状态机的维护，分发规则由线上系统实现
2. **状态拆分存储**：多 Key 便于按需查询，Flow Key 便于完整状态获取
3. **冷却期精确控制**：通过时间戳 + 印度时区对齐，确保冷却规则准确执行
4. **写放大控制**：只有明确反馈才写入，避免无效写入

### 9.2 与 V3 的关系

- **复用**：正/负反馈判定逻辑、最近 N 条记录聚合
- **新增**：状态机管理、冷却期计算、用户类型判定
- **不写入**：V3 的 `rtylevel_v3` 相关 key，避免干扰旧逻辑

### 9.3 扩展性

- **用户类型判定**：可插拔接口，支持 Redis/MySQL 查询
- **状态机规则**：集中在 `transition()` 方法，便于调整
- **冷却期规则**：集中在 `cooldownDaysByStage()` 和 `alignToNextIndia20()`，便于修改

