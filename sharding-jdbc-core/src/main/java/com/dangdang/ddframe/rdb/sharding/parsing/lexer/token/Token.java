package com.dangdang.ddframe.rdb.sharding.parsing.lexer.token;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 词法标记.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
@Getter
public final class Token {
    // 类型
    private final TokenType type;
    // 实际值
    private final String literals;
    // 字串中结束位置
    private final int endPosition;
}
