package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.config.RedisConnectionManager;
import com.gosh.job.UserFeatureCommon.PostViewEvent;
import com.gosh.job.UserFeatureCommon.PostViewInfo;
import com.gosh.job.UserFeatureCommon.ViewEventParser;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class UserPornLabelJobV3 {
    private static final Logger LOG = LoggerFactory.getLogger(UserPornLabelJobV3.class);

    // Redis Key 前缀和后缀
    private static final String REDIS_KEY_POS = "rec_post:{%d}:rtylevel_v3";               // key1: 正反馈最高等级
    private static final String REDIS_KEY_NEG = "rec_post:{%d}:rtylevel_v3_only_for_degree"; // key2: 负反馈最低等级

    private static final String CleanTag = "clean";
    private static final int REDIS_TTL_POS = 3 * 3600;     // key1: 3小时
    private static final int REDIS_TTL_NEG = 15 * 60;      // key2: 15分钟

    // Kafka Group ID
    private static final String KAFKA_GROUP_ID = "rec_porn_label_v3";

    // 需要详细日志监控的用户 UID 列表
    private static final Set<Long> MONITORED_UIDS = new HashSet<>(Arrays.asList(
            13659161L,  // 新用户
            13120233L,  // 老用户 / 专区用户
            13374748L,  // 非专区用户
            13661418L,   // 非专区用户（重度色情）
            13687026L,
            13372756L,
            13131686L,
            12103877L
    ));

    /**
     * 判断是否需要为指定 UID 输出详细日志
     */
    private static boolean shouldLogDetail(long uid) {
        return MONITORED_UIDS.contains(uid);
    }

    public static void main(String[] args) throws Exception {
        try {
            // 1. 创建 Flink 环境
            StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();

            // 2. 创建 Kafka Source
            Properties kafkaProperties = KafkaEnvUtil.loadProperties();
            kafkaProperties.setProperty("group.id", KAFKA_GROUP_ID);
            KafkaSource<String> kafkaSource = KafkaEnvUtil.createKafkaSource(
                    kafkaProperties,
                    "post"
            );

            // 3. 使用 KafkaSource 创建 DataStream
            DataStreamSource<String> kafkaStream = env.fromSource(
                    kafkaSource,
                    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                    "Kafka Source"
            );

            // 4. 预过滤 - 只保留观看事件（event_type=8）
            DataStream<String> filteredStream = kafkaStream
                    .filter(EventFilterUtil.createFastEventTypeFilter(8))
                    .name("Pre-filter View Events");

            // 5. 解析观看事件
            SingleOutputStreamOperator<PostViewEvent> viewStream = filteredStream
                    .flatMap(new ViewEventParser())
                    .name("Parse View Events");

            // 6.保留最近 N 条记录
            SingleOutputStreamOperator<UserNExposures> recentStats = viewStream
                    .keyBy(event -> event.uid)
                    .process(new RecentNExposures())
                    .map(event -> event)
                    .name("recent-exposure-statistics");

            // 7. 计算色情标签并输出两路Redis写入（key1正反馈、key2负反馈）
            DataStream<RedisWriteResult> writeResultStream = recentStats
                    .map(new PornLabelCalculatorV3())
                    .name("calc-porn-label-v3");

            DataStream<Tuple2<String, byte[]>> posStream = writeResultStream
                    .filter(r -> r != null && r.writePos)
                    .map(r -> Tuple2.of(r.posKey, r.posValue.getBytes()))
                    .returns(TypeInformation.of(new TypeHint<Tuple2<String, byte[]>>() {}))
                    .filter(new FilterFunction<Tuple2<String, byte[]>>() {
                        @Override
                        public boolean filter(Tuple2<String, byte[]> value) throws Exception {
                            if (value == null || value.f1 == null) {
                                return false;
                            }
                            // 将 byte[] 转换回 String 进行比较
                            String labelValue = new String(value.f1, java.nio.charset.StandardCharsets.UTF_8);
                            // 过滤掉 u_ylevel_unk，只保留 explicit、high、mid
                            if (!("u_ylevel_explicit".equals(labelValue) || "u_ylevel_high".equals(labelValue)
                                    || "u_ylevel_mid".equals(labelValue))) {
                                LOG.debug("过滤掉 非 u_ylevel_explicit｜u_ylevel_high｜u_ylevel_mid 标签，不写入 Redis: key={}", value.f0);
                                return false;
                            }
                            return true;
                        }
                    })
                    .name("pos-redis-stream");

            DataStream<Tuple2<String, byte[]>> negStream = writeResultStream
                    .filter(r -> r != null && r.writeNeg)
                    .map(r -> Tuple2.of(r.negKey, r.negValue.getBytes()))
                    .returns(TypeInformation.of(new TypeHint<Tuple2<String, byte[]>>() {}))
                    .filter(new FilterFunction<Tuple2<String, byte[]>>() {
                        @Override
                        public boolean filter(Tuple2<String, byte[]> value) throws Exception {
                            if (value == null || value.f1 == null) {
                                return false;
                            }
                            // 将 byte[] 转换回 String 进行比较
                            String labelValue = new String(value.f1, java.nio.charset.StandardCharsets.UTF_8);
                            // 过滤掉 u_ylevel_unk，只保留 explicit、high、mid
                            if (!("u_ylevel_explicit".equals(labelValue) || "u_ylevel_high".equals(labelValue)
                                    || "u_ylevel_mid".equals(labelValue))) {
                                LOG.debug("过滤掉 非 u_ylevel_explicit｜u_ylevel_high｜u_ylevel_mid 标签，不写入 Redis: key={}", value.f0);
                                return false;
                            }
                            return true;
                        }
                    })
                    .name("neg-redis-stream");

            // 8. 创建 Redis Sink（正/负TTL分别设置）
            RedisConfig redisConfigPos = RedisConfig.fromProperties(RedisUtil.loadProperties());
            redisConfigPos.setTtl(REDIS_TTL_POS);

            RedisConfig redisConfigNeg = RedisConfig.fromProperties(RedisUtil.loadProperties());
            redisConfigNeg.setTtl(REDIS_TTL_NEG);

            RedisUtil.addRedisSink(posStream, redisConfigPos, true, 100);
            RedisUtil.addRedisSink(negStream, redisConfigNeg, true, 100);

            // 执行任务
            env.execute("UserPornLabelJobV3");

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Flink任务执行失败", e);
            throw e;
        }
    }

    /**
     * 每个标签的统计信息（窗口内/最近N条）
     */
    /**
     * PostId的详细行为信息（用于日志）
     */
    public static class PostBehaviorDetail implements Serializable {
        public long postId;
        public float standingTime;
        public float progressTime;
        public List<String> positiveActions = new ArrayList<>();  // 正反馈行为列表（如：点赞、评论等）
        public List<String> negativeActions = new ArrayList<>();  // 负反馈行为列表（如：短播、dislike等）
        
        public PostBehaviorDetail(long postId, float standingTime, float progressTime) {
            this.postId = postId;
            this.standingTime = standingTime;
            this.progressTime = progressTime;
        }
    }

    public static class TagStatistics implements Serializable {
        public float standingTime = 0.0f;   // 总播放时长（所有postId的累加，保留字段）
        public float maxStandingTime = 0.0f; // 该标签下所有postId的最大时长（保留字段，暂未使用）
        public int shortPlayPostCount = 0;   // 该标签下时长<3秒的postId数量（用于负反馈判断）
        public int positiveCount = 0;       // 正反馈次数
        public int negativeCount = 0;       // 负反馈次数（包含短播 & dislike）
        public int shortPlayCount = 0;      // 短播次数（<3s，保留用于兼容）
        public int dislikeCount = 0;        // dislike 次数（interaction==11）
        public long triggerPostId = 0;      // 触发该标签统计的postId（用于日志）
        // 详细行为记录（用于日志）
        public List<PostBehaviorDetail> allPostDetails = new ArrayList<>();  // 所有postId的详情
        public List<PostBehaviorDetail> longPlayPostDetails = new ArrayList<>();  // standingTime >= 5秒的postId详情（用于正反馈判断）
        public List<PostBehaviorDetail> positivePostDetails = new ArrayList<>();  // 有明确正反馈行为的postId详情（点赞、评论等）
        public List<PostBehaviorDetail> negativePostDetails = new ArrayList<>();  // 有明确负反馈行为的postId详情（短播、dislike等）
    }

    /**
     * Redis 写入结果（正/负两路）
     */
    public static class RedisWriteResult implements Serializable {
        public boolean writePos;
        public boolean writeNeg;
        public String posKey;
        public String posValue;
        public String negKey;
        public String negValue;
        public long triggerPosPostId;  // 触发正反馈写入的postId
        public long triggerNegPostId;  // 触发负反馈写入的postId
    }

    /**
     * 计算色情标签并根据规则决定 key1/key2 的写入
     */
    public static class PornLabelCalculatorV3 extends RichMapFunction<UserNExposures, RedisWriteResult> {
        private transient RedisConnectionManager redisManager;

        @Override
        public void open(Configuration parameters) {
            redisManager = RedisConnectionManager.getInstance(RedisConfig.fromProperties(RedisUtil.loadProperties()));
        }

        @Override
        public RedisWriteResult map(UserNExposures event) throws Exception {
            long viewerId = event.viewer;
            boolean isMonitored = shouldLogDetail(viewerId);

            Map<String, TagStatistics> statsMap = aggregateStats(event, isMonitored);

            Tuple3<String, Long, TagStatistics> positiveResult = pickHighestPositive(statsMap, isMonitored, viewerId);
            String positiveTag = positiveResult.f0;
            long triggerPosPostId = positiveResult.f1;
            TagStatistics positiveStats = positiveResult.f2;

            Tuple3<String, Long, TagStatistics> negativeResult = pickLowestNegative(statsMap, isMonitored, viewerId);
            String negativeTag = negativeResult.f0;
            long triggerNegPostId = negativeResult.f1;
            TagStatistics negativeStats = negativeResult.f2;

            String posLabel = buildLabel(positiveTag);
            String negLabel = negativeTag == null ? null : buildLabel(negativeTag);

            String keyPos = String.format(REDIS_KEY_POS, viewerId);
            String keyNeg = String.format(REDIS_KEY_NEG, viewerId);

            String currentNegInRedis = readLabelFromRedis(keyNeg);
            String currentNegForUpdate = currentNegInRedis == null ? "u_ylevel_unk" : currentNegInRedis;
            String currentPosInRedis = readLabelFromRedis(keyPos);
            String currentPosForUpdate = currentPosInRedis == null ? "u_ylevel_unk" : currentPosInRedis;

            // 强制降级：当 key2 本次要写 explicit 或 high 时，如果当前 key1 存在且等级 >= mid，则强制设为 mid（降级或刷新TTL）
            // 如果 < mid，不处理（保持原逻辑）
            boolean negTriggeredPosUpdate = false;
            if (negLabel != null && currentPosInRedis != null) {
                boolean isExplicit = negLabel.contains("explicit");
                boolean isHigh = negLabel.contains("high");
                if (isExplicit || isHigh) {
                    int currentPosLevel = getLabelLevel(currentPosForUpdate);
                    final int MID_LEVEL = getLabelLevel("u_ylevel_mid");
                    if (currentPosLevel >= MID_LEVEL) {
                        posLabel = "u_ylevel_mid";
                        negTriggeredPosUpdate = true;
                        if (isMonitored) {
                            LOG.info("[监控用户 {}] key2={}触发，强制key1设为mid（原等级={}），触发postId={}", 
                                    viewerId, isExplicit ? "explicit" : "high", currentPosInRedis, triggerNegPostId);
                        }
                    }
                }
            }

            // 防护窗口：key2 当前为 explicit 或 high（15min TTL 内）时，不允许 key1 升级到 high/explicit
            boolean explicitGuardActive = (negLabel != null && (negLabel.contains("explicit") || negLabel.contains("high")))
                    || (currentNegInRedis != null && (currentNegInRedis.contains("explicit") || currentNegInRedis.contains("high")));
            final int MID_LEVEL = getLabelLevel("u_ylevel_mid");

            // key1（正反馈）写入判断：需要参考 key2 当前等级
            boolean writePos = false;
            if (!"u_ylevel_unk".equals(posLabel)) {
                int posLevel = getLabelLevel(posLabel);
                int negLevel = getLabelLevel(currentNegForUpdate);
                int posOldLevel = getLabelLevel(currentPosForUpdate);
                // 条件A：key2 不存在或 key1 新等级低于 key2 等级
                boolean passNeg = (currentNegInRedis == null) || (posLevel < negLevel);
                // 条件B：与原key1比较，新等级不低于原值（允许持平或升级；若显式防护，允许降级到 mid）
                boolean passPos = (currentPosInRedis == null) || (posLevel >= posOldLevel);

                // 防护：key2 为 explicit 或 high 时，禁止 key1 写入 high/explicit；允许写 mid/unk，并允许将高等级降到 mid
                if (explicitGuardActive) {
                    if (posLevel <= MID_LEVEL) {
                        passPos = true; // 允许覆盖为 mid（即便会降级）
                    } else {
                        passPos = false;
                    }
                }

                // 特殊情况：当负反馈为explicit或high触发强制降级时，如果正反馈已经是mid或更低，强制写入一次以刷新TTL
                if (negTriggeredPosUpdate && posLevel <= MID_LEVEL && currentPosInRedis != null) {
                    writePos = true;
                    if (isMonitored) {
                        LOG.info("[监控用户 {}] key2触发强制降级，强制刷新key1 TTL（posLabel={}），触发postId={}", 
                                viewerId, posLabel, triggerNegPostId);
                    }
                } else if (passNeg && passPos) {
                    writePos = true;
                } else if (isMonitored) {
                    LOG.info("[监控用户 {}] key1 不更新：posLevel={} vs negLevel={}, oldPosLevel={}, explicitGuard={}, negTriggered={}, 条件A(passNeg)={}, 条件B(passPos)={}",
                            viewerId, posLevel, negLevel, posOldLevel, explicitGuardActive, negTriggeredPosUpdate, passNeg, passPos);
                }
            }

            // key2（负反馈）写入判断：与原始 key2 比较，只有等级更低或相等才写
            boolean writeNeg = false;
            if (negLabel != null && !"u_ylevel_unk".equals(negLabel)) {
                String existingNeg = currentNegInRedis;
                if (existingNeg == null) {
                    writeNeg = true;
                } else {
                    int newNegLevel = getLabelLevel(negLabel);
                    int oldNegLevel = getLabelLevel(existingNeg);
                    if (newNegLevel <= oldNegLevel) {
                        writeNeg = true;
                    } else if (isMonitored) {
                        LOG.info("[监控用户 {}] key2 不更新：newNegLevel={} > oldNegLevel={}", viewerId, newNegLevel, oldNegLevel);
                    }
                }
            }

            if (isMonitored) {
                LOG.info("[监控用户 {}] 计算结果: posLabel={}, negLabel={}, writePos={}, writeNeg={}, keyPos={}, keyNeg={}",
                        viewerId, posLabel, negLabel, writePos, writeNeg, keyPos, keyNeg);
                if (writePos && positiveStats != null) {
                    // [色情标签写入] 统一关键字标识
                    LOG.info("[色情标签写入][监控用户 {}] ✓ key1正反馈写入Redis: key={}, value={}, 触发postId={}", 
                            viewerId, keyPos, posLabel, triggerPosPostId);
                    // 打印正反馈详细原因
                    StringBuilder detail = new StringBuilder();
                    detail.append("正反馈原因详情: ");
                    
                    // 如果有明确的正反馈行为（点赞、评论等），优先显示这些postId
                    if (!positiveStats.positivePostDetails.isEmpty()) {
                        detail.append("明确正反馈行为postId队列: ");
                        for (PostBehaviorDetail detailItem : positiveStats.positivePostDetails) {
                            detail.append(String.format("postId=%d(standingTime=%.2f秒, progressTime=%.2f秒", 
                                    detailItem.postId, detailItem.standingTime, detailItem.progressTime));
                            if (!detailItem.positiveActions.isEmpty()) {
                                detail.append(", 行为=").append(String.join(",", detailItem.positiveActions));
                            }
                            detail.append("); ");
                        }
                    }
                    
                    // 如果是所有postId都>=5秒触发的，显示所有满足条件的postId队列
                    boolean allPostLongPlay = !positiveStats.allPostDetails.isEmpty() && 
                            positiveStats.allPostDetails.size() == positiveStats.longPlayPostDetails.size();
                    if (allPostLongPlay && !positiveStats.longPlayPostDetails.isEmpty()) {
                        if (!positiveStats.positivePostDetails.isEmpty()) {
                            detail.append(" | ");
                        }
                        detail.append("长播postId队列(所有postId都>=5秒,共").append(positiveStats.longPlayPostDetails.size()).append("个): ");
                        for (PostBehaviorDetail detailItem : positiveStats.longPlayPostDetails) {
                            detail.append(String.format("postId=%d(standingTime=%.2f秒, progressTime=%.2f秒); ", 
                                    detailItem.postId, detailItem.standingTime, detailItem.progressTime));
                        }
                    }
                    
                    LOG.info("[色情标签写入][监控用户 {}] {}", viewerId, detail.toString());
                }
                if (writeNeg && negativeStats != null) {
                    // [色情标签写入] 统一关键字标识
                    LOG.info("[色情标签写入][监控用户 {}] ✓ key2负反馈写入Redis: key={}, value={}, 触发postId={}", 
                            viewerId, keyNeg, negLabel, triggerNegPostId);
                    // 打印负反馈详细原因：显示所有短播postId队列
                    if (!negativeStats.negativePostDetails.isEmpty()) {
                        StringBuilder detail = new StringBuilder();
                        detail.append("负反馈原因详情: ");
                        detail.append("短播postId队列(共").append(negativeStats.shortPlayPostCount).append("个): ");
                        for (PostBehaviorDetail detailItem : negativeStats.negativePostDetails) {
                            detail.append(String.format("postId=%d(standingTime=%.2f秒, progressTime=%.2f秒", 
                                    detailItem.postId, detailItem.standingTime, detailItem.progressTime));
                            if (!detailItem.negativeActions.isEmpty()) {
                                detail.append(", 行为=").append(String.join(",", detailItem.negativeActions));
                            }
                            detail.append("); ");
                        }
                        LOG.info("[色情标签写入][监控用户 {}] {}", viewerId, detail.toString());
                    }
                }
            }

            RedisWriteResult result = new RedisWriteResult();
            result.writePos = writePos;
            result.writeNeg = writeNeg;
            if (writePos) {
                result.posKey = keyPos;
                result.posValue = posLabel;
                result.triggerPosPostId = triggerPosPostId;
            }
            if (writeNeg) {
                result.negKey = keyNeg;
                result.negValue = negLabel;
                result.triggerNegPostId = triggerNegPostId;
            }
            return result;
        }

        private Map<String, TagStatistics> aggregateStats(UserNExposures event, boolean isMonitored) {
            Map<String, TagStatistics> statsMap = new HashMap<>();
            long filterTime = Instant.now().getEpochSecond() - (15 * 60);
            for (Tuple4<List<PostViewInfo>, Long, Long, String> tuple : event.firstNExposures) {
                String pornTag = tuple.f3;
                long postId = tuple.f1;
                TagStatistics stats = statsMap.getOrDefault(pornTag, new TagStatistics());
                // 记录触发该标签的postId（取第一个遇到的postId）
                if (stats.triggerPostId == 0) {
                    stats.triggerPostId = postId;
                }

                boolean hasPositiveBehavior = false;  // 标记当前postId是否有明确的正反馈行为（点赞、评论等）
                boolean hasNegativeBehavior = false;  // 标记当前postId是否有明确的负反馈行为（dislike等）
                boolean hasDislikeBehavior = false;  // 标记当前postId是否有dislike行为
                PostBehaviorDetail postDetail = null;  // 当前postId的行为详情
                float postTotalStandingTime = 0.0f;  // 当前postId的总时长（累加所有info的standingTime）
                float postTotalProgressTime = 0.0f;  // 当前postId的总进度时长（累加所有info的progressTime）
                
                // 第一步：聚合postId下所有info的standingTime和progressTime，收集行为信息
                for (PostViewInfo info : tuple.f0) {
                    if (info == null) {
                        continue;
                    }
                    // 累加总播放时长（保留字段，用于统计）
                    stats.standingTime += info.standingTime;
                    // 累加当前postId的总时长和总进度时长
                    postTotalStandingTime += info.standingTime;
                    postTotalProgressTime += info.progressTime;

                    // 初始化postId详情（使用第一个info初始化，后续会更新为总时长）
                    if (postDetail == null) {
                        postDetail = new PostBehaviorDetail(postId, 0.0f, 0.0f);
                    }

                    // 收集正反馈行为（沿用老规则）
                    if (tuple.f2 > filterTime) {
                        if (info.interaction != null && !info.interaction.isEmpty()) {
                            for (int action : info.interaction) {
                                if (isPositiveAction(action)) {
                                    // 只标记，不计数（避免重复计数）
                                    hasPositiveBehavior = true;
                                    String actionName = getActionName(action);
                                    if (!postDetail.positiveActions.contains(actionName)) {
                                        postDetail.positiveActions.add(actionName);
                                    }
                                } else if (action == 11 || action == 7 || action == 18) { // dislike / 不感兴趣
                                    // 只标记，不计数（避免重复计数）
                                    hasNegativeBehavior = true;
                                    if (action == 11) {
                                        hasDislikeBehavior = true;
                                        if (!postDetail.negativeActions.contains("dislike")) {
                                            postDetail.negativeActions.add("dislike");
                                        }
                                    } else if (action == 7) {
                                        if (!postDetail.negativeActions.contains("不感兴趣")) {
                                            postDetail.negativeActions.add("不感兴趣");
                                        }
                                    } else if (action == 18) {
                                        if (!postDetail.negativeActions.contains("不感兴趣")) {
                                            postDetail.negativeActions.add("不感兴趣");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                
                // 第二步：更新postId的总时长（聚合后的值）
                if (postDetail != null) {
                    postDetail.standingTime = postTotalStandingTime;
                    postDetail.progressTime = postTotalProgressTime;
                }
                
                // 第三步：基于postId的总时长进行正负反馈判断，并按postId计数（避免重复计数）
                boolean hasShortPlayInfo = false;  // 标记当前postId是否为短播（基于总时长判断）
                
                // 短播判断：使用postId的总时长（聚合后）< 3s 且总进度时长 < 3s 视为负反馈
                if (postTotalStandingTime < 3.0f && postTotalProgressTime < 3.0f) {
                    stats.shortPlayCount++;
                    hasShortPlayInfo = true;
                    hasNegativeBehavior = true;
                    if (postDetail != null && !postDetail.negativeActions.contains("短播")) {
                        postDetail.negativeActions.add("短播");
                    }
                }
                
                // 按postId计数（每个postId只计数一次，避免重复计数）
                if (hasPositiveBehavior) {
                    stats.positiveCount++;  // 该postId有正反馈行为，计数+1
                }
                if (hasNegativeBehavior) {
                    stats.negativeCount++;  // 该postId有负反馈行为，计数+1
                }
                if (hasDislikeBehavior) {
                    stats.dislikeCount++;  // 该postId有dislike行为，计数+1
                }
                
                // 如果当前postId是短播，则计入短播postId数量（用于负反馈判断）
                if (hasShortPlayInfo) {
                    stats.shortPlayPostCount++;
                }
                
                // 记录postId的详细行为信息（用于日志）
                if (postDetail != null) {
                    // 所有postId都记录
                    stats.allPostDetails.add(postDetail);
                    // 判断当前postId的总时长（累加所有info的standingTime）是否 >= 5秒
                    // 用于正反馈判断：所有postId都 >= 5秒
                    if (postDetail.standingTime >= 5.0f) {
                        stats.longPlayPostDetails.add(postDetail);
                    }
                    // 有明确正反馈行为的postId（点赞、评论等）
                    if (hasPositiveBehavior) {
                        stats.positivePostDetails.add(postDetail);
                    }
                    // 有明确负反馈行为的postId（短播、dislike等）
                    // 注意：所有短播的postId都需要记录，即使没有其他负反馈行为
                    if (hasNegativeBehavior || hasShortPlayInfo) {
                        // 如果是短播但没有其他负反馈行为，确保negativeActions包含"短播"
                        if (hasShortPlayInfo && !hasNegativeBehavior) {
                            if (!postDetail.negativeActions.contains("短播")) {
                                postDetail.negativeActions.add("短播");
                            }
                        }
                        stats.negativePostDetails.add(postDetail);
                    }
                }

                statsMap.put(pornTag, stats);
            }

            if (isMonitored) {
                LOG.info("[监控用户 {}] 标签统计结果: {}", event.viewer, statsMap);
            }
            return statsMap;
        }

        private boolean isPositiveAction(int action) {
            // 与V2一致的正反馈集合
            return action == 1 || action == 3 || action == 5 || action == 6 || action == 9
                    || action == 10 || action == 13 || action == 15 || action == 16;
        }
        
        /**
         * 获取行为名称（用于日志）
         */
        private String getActionName(int action) {
            switch (action) {
                case 1: return "点赞";
                case 3: return "评论";
                case 5: return "分享";
                case 6: return "收藏";
                case 9: return "下载";
                case 10: return "购买";
                case 13: return "关注";
                case 15: return "查看主页";
                case 16: return "订阅";
                default: return "正反馈行为" + action;
            }
        }

        private Tuple3<String, Long, TagStatistics> pickHighestPositive(Map<String, TagStatistics> statsMap, boolean isMonitored, long uid) {
            String bestTag = null;
            long bestPostId = 0;
            int bestLevel = -1;
            TagStatistics bestStats = null;
            for (Map.Entry<String, TagStatistics> entry : statsMap.entrySet()) {
                String tag = entry.getKey();
                TagStatistics s = entry.getValue();
                if ("unk".equals(tag) || CleanTag.equals(tag)) {
                    continue;
                }
                // 正反馈判断：所有postId的standingTime都 >= 5秒 或 有正反馈行为
                // 检查是否所有postId都满足时长条件（>= 5秒）
                boolean allPostLongPlay = !s.allPostDetails.isEmpty() && 
                        s.allPostDetails.size() == s.longPlayPostDetails.size();
                boolean positive = allPostLongPlay || (s.positiveCount > 0);
                boolean noNegative = s.negativeCount <= 0;
                if (positive && noNegative) {
                    int level = getTagLevel(tag);
                    if (level > bestLevel) {
                        bestLevel = level;
                        bestTag = tag;
                        bestPostId = s.triggerPostId;
                        bestStats = s;
                    }
                }
            }
            if (isMonitored) {
                LOG.info("[监控用户 {}] 正反馈候选: bestTag={}, level={}, triggerPostId={}", uid, bestTag, bestLevel, bestPostId);
            }
            return Tuple3.of(bestTag, bestPostId, bestStats);
        }

        private Tuple3<String, Long, TagStatistics> pickLowestNegative(Map<String, TagStatistics> statsMap, boolean isMonitored, long uid) {
            String worstTag = null;
            long worstPostId = 0;
            int worstLevel = Integer.MAX_VALUE; // 越低越好（explicit高，unk低）
            TagStatistics worstStats = null;
            for (Map.Entry<String, TagStatistics> entry : statsMap.entrySet()) {
                String tag = entry.getKey();
                TagStatistics s = entry.getValue();
                if ("unk".equals(tag) || CleanTag.equals(tag)) {
                    continue;
                }
                // 负反馈触发条件：该标签下有5个postId的时长<3秒 或 dislike >=1
                if (s.shortPlayPostCount >= 5 || s.dislikeCount >= 1) {
                    int level = getTagLevel(tag);
                    if (level < worstLevel) {
                        worstLevel = level;
                        worstTag = tag;
                        worstPostId = s.triggerPostId;
                        worstStats = s;
                    }
                }
            }
            if (isMonitored) {
                LOG.info("[监控用户 {}] 负反馈候选: worstTag={}, level={}, triggerPostId={}", uid, worstTag, worstLevel, worstPostId);
            }
            return Tuple3.of(worstTag, worstPostId, worstStats);
        }

        private String readLabelFromRedis(String key) {
            try {
                Tuple2<String, byte[]> tuple = redisManager.getStringCommands().get(key);
                if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                    return new String(tuple.f1, java.nio.charset.StandardCharsets.UTF_8);
                }
            } catch (Exception e) {
                LOG.warn("读Redis失败 key={}, err={}", key, e.getMessage());
            }
            return null;
        }
    }

    // ===== 辅助函数 =====
    private static int getTagLevel(String tag) {
        if (tag == null) return 0;
        if (tag.contains("explicit")) return 3;
        if (tag.contains("high")) return 2;
        if (tag.contains("mid")) return 1;
        return 0;
    }

    private static int getLabelLevel(String label) {
        if (label == null) return 0;
        if (label.contains("explicit")) return 3;
        if (label.contains("high")) return 2;
        if (label.contains("mid")) return 1;
        return 0;
    }

    private static String buildLabel(String tag) {
        if (tag == null || tag.isEmpty()) {
            return "u_ylevel_unk";
        }
        return "u_ylevel_" + tag;
    }

    // 前N次曝光统计输出
    public static class UserNExposures  implements Serializable {
        public long viewer;
        public List<Tuple4<List<PostViewInfo>, Long, Long, String>>  firstNExposures;  // 前N次曝光记录
        public long collectionTime;  // 收集完成时间

        public UserNExposures(long viewer, List<Tuple4<List<PostViewInfo>, Long, Long, String>> exposures) {
            this.viewer = viewer;
            if (exposures == null || exposures.isEmpty()) {
                this.firstNExposures = new ArrayList<>();
            } else {
                this.firstNExposures = exposures;
            }
            this.collectionTime = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return String.format("User %d collected %s exposures", viewer, firstNExposures.toString());
        }
    }

    static class RecentNExposures extends KeyedProcessFunction<Long, PostViewEvent, UserNExposures> {
        private static final int N = 30;  // 保留最近30次
        private static final int CalNum = 2;  // 满足 2 条就
        private transient ListState<Tuple4<List<PostViewInfo>, Long, Long, String>> recentViewEventState;  //
        private transient RedisConnectionManager redisManager;

        @Override
        public void open(Configuration parameters) {
            redisManager = RedisConnectionManager.getInstance(RedisConfig.fromProperties(RedisUtil.loadProperties()));
//            recentViewEventState = getRuntimeContext().getListState(
//                    new ListStateDescriptor<>("recentViewEvent",
//                            org.apache.flink.api.common.typeinfo.Types.TUPLE(
//                                    org.apache.flink.api.common.typeinfo.Types.LIST(org.apache.flink.api.common.typeinfo.Types.GENERIC(PostViewInfo.class)),
//                                    org.apache.flink.api.common.typeinfo.Types.LONG,
//                                    org.apache.flink.api.common.typeinfo.Types.LONG,
//                                    org.apache.flink.api.common.typeinfo.Types.STRING
//                            )
//                    )
//            );

            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1)) // 设置状态存活时间为1小时
                    .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime) // 使用处理时间（也可用EventTime）
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 仅在创建和写入时更新TTL
                    .cleanupFullSnapshot() // 清理策略：全量快照时清理（适用RocksDB和文件系统后端）
                    .disableCleanupInBackground() // 可选：禁用后台清理（某些场景需要）
                    .build();

            // 2. 创建状态描述符，并为其启用 TTL
            ListStateDescriptor<Tuple4<List<PostViewInfo>, Long, Long, String>> descriptor =
                    new ListStateDescriptor<>(
                            "recentViewEvent",
                            TypeInformation.of(new TypeHint<Tuple4<List<PostViewInfo>, Long, Long, String>>() {})
                    );

            descriptor.enableTimeToLive(ttlConfig); // 关键：启用TTL

            // 3. 获取状态
            recentViewEventState = getRuntimeContext().getListState(descriptor);

        }
        @Override
        public void close() throws Exception {
            if (redisManager != null) {
                redisManager.shutdown();
            }
            super.close();
        }

        @Override
        public void processElement(PostViewEvent event, Context ctx, Collector<UserNExposures> out) throws Exception {

            long currentTime = ctx.timestamp();
            long viewerId = event.uid;
            boolean isMonitored = shouldLogDetail(viewerId);

            if (isMonitored) {
                LOG.info("========== [监控用户 {}] 收到新事件 ==========", viewerId);
                LOG.info("[监控用户 {}] 事件时间戳: {}", viewerId, currentTime);
                LOG.info("[监控用户 {}] 事件创建时间: {}", viewerId, event.createdAt);
                LOG.info("[监控用户 {}] PostViewInfo 数量: {}", viewerId, event.infoList.size());
                for (int i = 0; i < event.infoList.size(); i++) {
                    PostViewInfo info = event.infoList.get(i);
                    LOG.info("[监控用户 {}]   PostViewInfo #{}: postId={}, standingTime={}, progressTime={}, author={}, recToken={}, interactions={}", 
                            viewerId, i + 1, info.postId, info.standingTime, info.progressTime, info.author, info.recToken, info.interaction);
                }
            }

            // 获取当前所有记录
            List<Tuple4<List<PostViewInfo>, Long, Long, String>> allRecords = new ArrayList<>();

            for (Tuple4<List<PostViewInfo>, Long, Long, String> record : recentViewEventState.get()) {
                allRecords.add(record);
            }

            if (isMonitored) {
                LOG.info("[监控用户 {}] 当前状态中的记录数: {}", viewerId, allRecords.size());
                for (int i = 0; i < allRecords.size(); i++) {
                    Tuple4<List<PostViewInfo>, Long, Long, String> record = allRecords.get(i);
                    LOG.info("[监控用户 {}]   状态记录 #{}: postId={}, expoTime={}, pornTag={}, PostViewInfo数量={}", 
                            viewerId, i + 1, record.f1, record.f2, record.f3, record.f0.size());
                }
            }

            // 添加新记录，包含序号
            for (PostViewInfo info : event.infoList) { // 按 postid 归类
                boolean exist = false;
                for (Tuple4<List<PostViewInfo>, Long, Long, String> record : allRecords) {
                    if (record.f1 == info.postId) { // 已存在
                        if (isMonitored) {
                            LOG.info("[监控用户 {}] PostId {} 已存在，追加 PostViewInfo", viewerId, info.postId);
                        }
                        record.f0.add(info);
                        exist = true;
                        break;
                    }
                }
                if (!exist) {
                    if (isMonitored) {
                        LOG.info("[监控用户 {}] PostId {} 不存在，从 Redis 读取标签", viewerId, info.postId);
                    }
                    String pornTag = getPostTagFromRedis(info.postId, isMonitored).get();
                    List<PostViewInfo> infos = new ArrayList<>();
                    infos.add(info);
                    allRecords.add(new Tuple4<>(
                            infos,
                            info.postId,
                            event.createdAt,
                            pornTag
                    ));
                    if (isMonitored) {
                        LOG.info("[监控用户 {}] 新增记录: postId={}, expoTime={}, pornTag={}", 
                                viewerId, info.postId, event.createdAt, pornTag);
                    }
                }
            }

            // 按 num 排序（如果需要）
            allRecords.sort(Comparator.comparing(r -> r.f2));

            if (isMonitored) {
                LOG.info("[监控用户 {}] 排序后的记录数: {}", viewerId, allRecords.size());
                for (int i = 0; i < allRecords.size(); i++) {
                    Tuple4<List<PostViewInfo>, Long, Long, String> record = allRecords.get(i);
                    LOG.info("[监控用户 {}]   排序后记录 #{}: postId={}, expoTime={}, pornTag={}, PostViewInfo数量={}", 
                            viewerId, i + 1, record.f1, record.f2, record.f3, record.f0.size());
                }
            }

            // 只保留最近N条记录
            int originalSize = allRecords.size();
            if (allRecords.size() > N) {
                allRecords = new ArrayList<>(allRecords.subList(allRecords.size() - N, allRecords.size()));
                if (isMonitored) {
                    LOG.info("[监控用户 {}] 记录数 {} > N({})，截取最近 {} 条", viewerId, originalSize, N, allRecords.size());
                }
            } else {
                if (isMonitored) {
                    LOG.info("[监控用户 {}] 记录数 {} <= N({})，保留全部", viewerId, allRecords.size(), N);
                }
            }

            // 更新状态
            recentViewEventState.clear();
            recentViewEventState.addAll(allRecords);

            if (isMonitored) {
                LOG.info("[监控用户 {}] 状态已更新，当前记录数: {}", viewerId, allRecords.size());
            }

            // 输出最新状态
            if (allRecords.size() >= CalNum) {
                UserNExposures result = new UserNExposures(viewerId, allRecords);
                if (isMonitored) {
                    LOG.info("[监控用户 {}] ✓ 满足输出条件 (记录数 {} >= CalNum {})，输出 UserNExposures", 
                            viewerId, allRecords.size(), CalNum);
                    LOG.info("[监控用户 {}] 输出结果: viewer={}, exposureCount={}, collectionTime={}", 
                            viewerId, result.viewer, result.firstNExposures.size(), result.collectionTime);
                }
                out.collect(result);
            } else {
                if (isMonitored) {
                    LOG.info("[监控用户 {}] ✗ 不满足输出条件 (记录数 {} < CalNum {})，不输出", 
                            viewerId, allRecords.size(), CalNum);
                }
            }
        }
        String UNK = "unk";
        private CompletableFuture<String> getPostTagFromRedis(long postId) {
            return getPostTagFromRedis(postId, false);
        }
        
        private CompletableFuture<String> getPostTagFromRedis(long postId, boolean isMonitored) {
            String redisKey = "rec_post:{" + postId + "}:aitag";
            
            if (isMonitored) {
                LOG.info("========== [监控用户] Redis 读取 ==========");
                LOG.info("Redis Key: {}", redisKey);
            }
            
            return redisManager.executeStringAsync(
                    commands -> {
                        try {
                            Tuple2<String, byte[]> tuple = commands.get(redisKey);
                            if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                                String value = new String(tuple.f1, java.nio.charset.StandardCharsets.UTF_8);
                                
                                if (isMonitored) {
                                    LOG.info("Redis Value (原始): {}", value);
                                    LOG.info("Redis Value (bytes): {}", java.util.Arrays.toString(tuple.f1));
                                }
                                
                                if (value.isEmpty()) {
                                    if (isMonitored) {
                                        LOG.info("Redis Value 为空，返回默认标签: {}", UNK);
                                    }
                                    return UNK;
                                }
                                
                                String selectedTag = selectPornTag(value, postId, isMonitored);
                                if (isMonitored) {
                                    LOG.info("最终选择的标签: {}", selectedTag);
                                }
                                return selectedTag;
                            } else {
                                if (isMonitored) {
                                    LOG.info("Redis Key 不存在或值为空，返回默认标签: {}", UNK);
                                }
                            }
                        } catch (Exception e) {
                            LOG.warn("Failed to fetch tag from Redis for postId {}: {}", postId, e.getMessage());
                            if (isMonitored) {
                                LOG.error("[监控用户] Redis 读取异常，postId={}, error={}", postId, e.getMessage(), e);
                            }
                        }
                        return UNK;
                    }
            );
        }
        
        private String selectPornTag(String rawValue) {
            return selectPornTag(rawValue, -1, false);
        }
        
        private String selectPornTag(String rawValue, long postId, boolean isMonitored) {
            if (isMonitored) {
                LOG.info("========== [监控用户] 标签解析 ==========");
                LOG.info("PostId: {}", postId);
                LOG.info("原始标签字符串: {}", rawValue);
            }
            
            String[] tags = rawValue.split(",");
            String contentCandidate = UNK;
            
            if (isMonitored) {
                LOG.info("分割后的标签数组 (数量: {}): {}", tags.length, java.util.Arrays.toString(tags));
            }
            
            for (int i = 0; i < tags.length; i++) {
                String tag = tags[i];
                if (tag == null) {
                    if (isMonitored) {
                        LOG.info("  标签 #{}: null (跳过)", i + 1);
                    }
                    continue;
                }
                String trimmed = tag.trim();
                if (trimmed.isEmpty()) {
                    if (isMonitored) {
                        LOG.info("  标签 #{}: 空字符串 (跳过)", i + 1);
                    }
                    continue;
                }
                
                if (isMonitored) {
                    LOG.info("  标签 #{}: {}", i + 1, trimmed);
                }
                
                if (trimmed.contains("restricted#")) {
                    if (isMonitored) {
                        LOG.info("    包含 'restricted#'，开始解析");
                    }
                    String[] vals = trimmed.split("#");
                    if (isMonitored) {
                        LOG.info("    分割结果: {}", java.util.Arrays.toString(vals));
                    }
                    if (vals.length == 2) {
                        String tagType = vals[1];
                        if (isMonitored) {
                            LOG.info("    标签类型: {}", tagType);
                        }
                        if (CleanTag.equals(tagType)) {
                            if (isMonitored) {
                                LOG.info("    → 匹配到 clean，返回: {}", CleanTag);
                            }
                            return CleanTag;
                        } else if ("explicit".equals(tagType)) {
                            if (isMonitored) {
                                LOG.info("    → 匹配到 explicit，返回: explicit");
                            }
                            return "explicit";
                        } else if ("borderline".equals(tagType) || "mid".equals(tagType)) {
                            if (isMonitored) {
                                LOG.info("    → 匹配到 borderline 或 mid，返回: mid");
                            }
                            return "mid";
                        } else if ("mid-sexy".equals(tagType) || "high".equals(tagType)) {
                            if (isMonitored) {
                                LOG.info("    → 匹配到 mid-sexy 或 high，返回: high");
                            }
                            return "high";
                        } else {
                            if (isMonitored) {
                                LOG.info("    → 未知标签类型: {}，返回: {}", tagType, UNK);
                            }
                            return UNK;
                        }
                    } else {
                        if (isMonitored) {
                            LOG.info("    分割后长度 != 2，跳过");
                        }
                    }
                    break;
                }
            }
            
            if (isMonitored) {
                LOG.info("未找到匹配的 restricted# 标签，返回默认: {}", contentCandidate);
            }
            return contentCandidate;
        }

    }

}