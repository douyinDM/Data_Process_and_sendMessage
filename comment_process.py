# %%
import jieba.posseg as pseg
import re
import pandas as pd
from snownlp import SnowNLP
from collections import Counter
import match
from sqlalchemy import create_engine
import db_connect as dbc

# %%
# 全局参数配置
CONFIG = {
    "keywords": {
        "explicit": {'律师', '咨询', '起诉', '赔偿', '费用', '维权', '仲裁', '诉讼', '公证'},
        "implicit": {'怎么办', '合法吗', '如何', '求助', '解答', '请教', '该不该', '能不能', '是不是违法'},
        "domain": {'工伤', '离婚', '合同', '劳动法', '遗产', '房产', '交通事故', '借贷', '抚养权'},
        "urgent": {'紧急', '救命', '在线等', '今天就要', '非常急', '立刻', '马上', '急求'}
    },
    "weights": {
        "explicit": 1.2,
        "implicit": 0.8,
        "domain": 1.0,
        "urgent": 1.5,
        "sentiment": 0.4,
        "length": 0.1,
        "question": 0.1,
        "special_question": 0.3
    },
    "thresholds": {
        "S级": 0.85,
        "A级": 0.7,
        "B级": 0.5
    }
}

DOMAIN_MAPPING = {
    "劳动纠纷": {"劳动合同", "工资", "辞退", "社保", "工伤", "加班费", "赔偿金"},
    "婚姻家庭": {"离婚", "抚养权", "财产分割", "继承", "赡养", "扶养费"},
    "民事侵权": {"交通事故", "伤残", "工伤", "人身损害", "名誉权", "隐私权"},
    "合同纠纷": {"违约", "合同", "协议", "退款", "租赁", "保证金", "违约金"},
    "债务纠纷": {"借贷", "欠款", "欠钱", "还款", "贷款", "信用卡", "催收"},
    "刑事案件": {"诈骗", "盗窃", "伤害", "家暴", "刑事", "犯罪", "报警"},
    "房产纠纷": {"房产", "物业", "租赁", "买房", "卖房", "公摊", "拆迁"},
    "法律咨询": set()
}

CHINA_REGIONS = {"北京", "上海", "广东", "江苏", "浙江", "山东", "福建", "四川", "重庆",
                 "湖北", "湖南", "安徽", "江西", "天津", "广西", "陕西", "辽宁", "吉林",
                 "黑龙江", "山西", "河南", "河北", "贵州", "云南", "甘肃", "青海", "海南",
                 "内蒙古", "宁夏", "西藏", "新疆", "中国香港", "中国澳门", "中国台湾"}


# %%
def preprocess_text(text: str) -> list:
    """文本预处理函数"""
    text = re.sub(r'[^\u4e00-\u9fa50-9?！]', '', text).strip()
    if len(text) < 4:
        return []

    words = pseg.lcut(text)
    stopwords = {'的', '了', '啊', '吗', '呢', '这个', '请问', '是不是', '大家', '觉得', '什么', '有没有', '可以',
                 '如何'}
    return [word.word for word in words if word.word not in stopwords and len(word.word) > 1]


# %%
def determine_domain(content: str) -> str:
    """领域细分函数"""
    words = preprocess_text(content)
    word_freq = Counter(words)

    domain_scores = {cat: sum(word_freq[w] for w in keywords) for cat, keywords in DOMAIN_MAPPING.items()}
    best_match = max(domain_scores, key=domain_scores.get)

    return best_match if domain_scores[best_match] > 0 else "法律咨询"


# %%
def extract_features(content: str) -> dict:
    """特征提取函数"""
    features = {}
    words = preprocess_text(content)
    word_freq = Counter(words)

    for key in CONFIG["keywords"]:
        features[key] = sum(word_freq[w] for w in CONFIG["keywords"][key]) * CONFIG["weights"][key]

    try:
        s = SnowNLP(content)
        negative_sentiment = 1 - s.sentiments
        punctuations = re.findall(r'[？！]', content)
        punctuation_density = min(len(punctuations) / len(content), 0.2)
        features["sentiment"] = min(negative_sentiment + punctuation_density, 1.0) * CONFIG["weights"]["sentiment"]
    except Exception:
        features["sentiment"] = 0.3

    last_char = content[-1] if content else ''
    features["special_question"] = CONFIG["weights"]["special_question"] if last_char in {'吗', '呢', '如何', '觉得',
                                                                                          '是不是', '怎么'} else 0.0
    features["has_question"] = (1.0 if '？' in content or '！' in content else 0.0) * CONFIG["weights"]["question"]
    features["length"] = min(len(content) / 150, 1.0) * CONFIG["weights"]["length"]

    return features


# %%
def calculate_score(features: dict) -> float:
    """需求指数计算函数"""
    base_score = sum(features[key] for key in ["explicit", "implicit", "domain"]) / 3
    urgency_factor = 1 + min(features["urgent"], 1.0)

    return max(0.0, min(
        base_score * urgency_factor +
        features["sentiment"] +
        features["length"] +
        features["has_question"] +
        features["special_question"],
        1.0
    ))


# %%
def get_action_advice(score: float) -> str:
    """处理建议生成函数"""
    if score >= CONFIG["thresholds"]["S级"]:
        return "【S级】人工介入"
    elif score >= CONFIG["thresholds"]["A级"]:
        return "【A级】发送咨询问卷"
    elif score >= CONFIG["thresholds"]["B级"]:
        return "【B级】自动回复法律指南"
    else:
        return "【常规】引导查看常见问题"


# %%
def detect_foreign_ips(df, ip_col="ip_location", user_col="nickname", content_col="content"):
    """境外IP检测函数"""
    return df[~df[ip_col].astype(str).str.contains("|".join(CHINA_REGIONS), na=False)][[user_col, ip_col, content_col]]

# %%
def detect_repeated_comments(df):
    """重复评论检测函数"""
    df["重复评论次数"] = df.groupby(["nickname", "content"])["content"].transform("count")
    return df[df["重复评论次数"] > 1][["nickname", "content", "重复评论次数"]].drop_duplicates()

# %%
def process_comments(table_name: str):
    try:
        engine = dbc.get_db_engine()
        if not engine:
            return
        # 从数据库中读取评论数据
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, engine)

        # 确保评论数据有 'content' 列
        if "content" not in df.columns:
            raise ValueError("数据库中的表缺少评论内容列 'content'")

        # 填充空值并进行数据类型转换
        df["content"] = df["content"].fillna("").astype(str)

        # 计算需求指数并生成建议
        df["demand_index"] = df["content"].apply(lambda x: calculate_score(extract_features(x)))
        df["advice"] = df["demand_index"].apply(get_action_advice)
        df["field"] = df["content"].apply(determine_domain)

        # 筛选出需求指数较高的评论
        filtered_df = df[df["advice"].str.contains("S级|A级|B级")].copy()

        # 格式化时间戳
        filtered_df["create_time"] = pd.to_datetime(filtered_df["create_time"], unit='s', utc=True).dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d %H:%M:%S')
        # 创建新的 DataFrame，包含所需的字段
        output_df = pd.DataFrame({
            "telephone_number": "", "wechat": "", "QQ": "",
            "douyin_id": filtered_df["user_unique_id"],
            "another_accounts": "",
            "aweme_id": filtered_df["aweme_id"],
            "user_nickname": filtered_df["nickname"], "gender": "",
            "comment_time": filtered_df["create_time"],
            "ip_location": filtered_df["ip_location"],
            "industry": "法律",
            "filed": filtered_df["field"],
            "comment": filtered_df["content"],
            "advice": filtered_df["advice"],
            "demand_index": filtered_df["demand_index"],
        })
        return output_df

    except Exception as e:
        print(f"❌ 从数据库读取或处理数据出错: {e}")
        return None
