package com.gosh.cons;

// 倾斜类型枚举
public enum SkewType {
    HOT_KEY,       // 热点Key倾斜
    PARTITION_SKEW, // 分区倾斜
    GLOBAL_SKEW,   // 全局倾斜
    NONE           // 无倾斜
}