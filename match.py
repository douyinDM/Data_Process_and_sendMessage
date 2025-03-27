import pandas as pd
from sqlalchemy import create_engine, text
import db_connect as dbc

def create_processed_result_table():
    """检查 MySQL 是否存在 processed_result 表，如果不存在则创建"""
    try:
        engine = dbc.get_db_engine()
        if not engine:
            return

        with engine.connect() as conn:
            # 检查表是否存在
            result = conn.execute(text("SHOW TABLES LIKE 'processed_result';")).fetchone()
            if result:
                print("✅ 表 `processed_result` 已存在，无需创建")
            else:
                # 创建表 SQL 语句
                create_table_sql = """
                CREATE TABLE processed_result (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    aweme_id VARCHAR(50),
                    title LONGTEXT,
                    aweme_url TEXT,
                    source_keyword VARCHAR(255),
                    telephone_number VARCHAR(50),
                    wechat VARCHAR(50),
                    QQ VARCHAR(50),
                    douyin_id VARCHAR(50),
                    another_accounts TEXT,
                    user_nickname VARCHAR(255),
                    gender VARCHAR(10),
                    comment_time DATETIME,
                    ip_location VARCHAR(255),
                    industry VARCHAR(255),
                    filed VARCHAR(255),
                    comment TEXT,
                    advice TEXT,
                    demand_index INT
                );
                """
                conn.execute(text(create_table_sql))
                print("✅ 表 `processed_result` 创建成功")
    except Exception as e:
        print(f"❌ 创建 `processed_result` 表时出错: {e}")


def match_and_save_data(output_df,table_names_video):
    """匹配 filtered_df 和 视频数据，根据 aweme_id 写入 processed_result 表"""
    try:
        engine = dbc.get_db_engine()
        if not engine:
            return

        # 读取视频数据
        query = f"SELECT aweme_id, title, aweme_url, source_keyword FROM {table_names_video};"  #只选择需要的列
        df = pd.read_sql(query, engine)

        # 使用左连接（左边是 output_df，右边是视频数据）
        merged_df = pd.merge(output_df, df, on="aweme_id", how="left")

        # 调整列顺序，将视频信息的四列放到前面
        merged_df = merged_df[['aweme_id', 'title', 'aweme_url', 'source_keyword'] + [col for col in merged_df.columns if col not in ['aweme_id', 'title', 'aweme_url', 'source_keyword']]]

        # 确保 processed_result 表已存在
        create_processed_result_table()

        # 将合并后的数据存入 processed_result 表
        merged_df.to_sql(name="processed_result", con=engine, if_exists="replace", index=False)

        print("✅ 数据已成功存入 `processed_result` 表！")

    except Exception as e:
        print(f"❌ 数据处理和存储失败: {e}")
