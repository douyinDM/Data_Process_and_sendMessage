import yaml
from sqlalchemy import create_engine

# 获取 SQLAlchemy 数据库引擎，首次连接数据库获取评论视频数据
def get_db_engine():
    try:
        # 读取配置文件
        with open("config.yaml", 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)["database"]

        # 确保所有参数为字符串
        connection_params = {key: str(config.get(key, '')) for key in ["host", "user", "password", "database", "port"]}

        # 创建 SQLAlchemy 引擎
        engine = create_engine(
            f"mysql+pymysql://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}")

        print("✅ 首次数据库连接成功")
        return engine  # 🚀 确保返回的是 `engine`，而不是 `mysql.connector`

    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return None
