import mysql.connector
import yaml

#其次连接数据库获取已处理数据
def connect_to_db():
    try:
        # 读取配置文件
        with open("config.yaml", 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)["database"]

        # 使用 for 循环动态提取并确保所有参数为字符串类型
        connection_params = {}
        for key in ["host", "user", "password", "database"]:
            connection_params[key] = str(config.get(key, ''))  # 确保每个配置项为字符串

        # 连接到 MySQL 数据库
        connection = mysql.connector.connect(**connection_params)

        # 检查连接是否成功
        if connection.is_connected():
            print("✅ 二次数据库连接成功")
            return connection
        else:
            print("❌ 数据库连接失败")
            return None
    except mysql.connector.Error as err:
        print(f"❌ 数据库连接失败: {err}")
        return None
    except Exception as e:
        print(f"❌ 配置加载失败: {e}")
        return None

# if __name__ == '__main__':
#     connect_to_db()