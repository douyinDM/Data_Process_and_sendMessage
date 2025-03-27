#%%
#导入依赖
from appium.options.common import AppiumOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import yaml
import time
#%%
#启动参数配置
def config_settings():
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    options = AppiumOptions()
    for key, value in config.items():
        options.set_capability(key, value)
    return options
#%%
#创建状态列判断抖音号是否已处理
def ensure_status_column(connections):
    cursor = connections.cursor()
    cursor.execute("SHOW COLUMNS FROM processed_result LIKE 'status'")
    result = cursor.fetchone()

    if not result:
        cursor.execute("ALTER TABLE processed_result ADD COLUMN status VARCHAR(20) DEFAULT 'pending'")
        print("✅ 已创建 `status` 列")
    else:
        print("✅ `status` 列已存在")

    connections.commit()

#%%
#从数据库获取抖音号
def get_douyin_ids(connections):
    cursor = connections.cursor()
    cursor.execute("SELECT douyin_id FROM processed_result WHERE status = 'pending' AND douyin_id IS NOT NULL AND douyin_id != '' LIMIT 1")
    result = cursor.fetchone()
    if result:
        return result[0]  # 返回第一个抖音号
    else:
        return None
#%%
#更新抖音号状态为已处理
def update_douyin_status(connections, douyin_ids):
    cursor = connections.cursor()
    cursor.execute(
        "UPDATE processed_result SET status = 'done' WHERE douyin_id = %s", (douyin_ids,)
    )
    connections.commit()
    print(f"✅ 抖音号 {douyin_ids} 处理完成，已标记为 'done'")
#%%
#处理完成后删除状态列
def drop_status_column(connections):
    cursor = connections.cursor()
    cursor.execute("ALTER TABLE processed_result DROP COLUMN status")
    connections.commit()
    print("✅ 已删除 `status` 列")
#%%
#启动抖音打开个人主页并进入添加朋友界面
def init_and_open_home_page(drivers):
    try:
        # 等待抖音首页加载完成
        content_elements = WebDriverWait(drivers, 10).until(
            EC.presence_of_all_elements_located((By.ID, "com.ss.android.ugc.aweme:id/qal"))
        )
        if content_elements:
            print("✅ 成功进入抖音首页")
        else:
            print("❌ 未能成功加载抖音首页")
            return

        # 点击主页元素，进入个人主页
        home_button = WebDriverWait(drivers, 10).until(
            EC.presence_of_all_elements_located((By.ID, "com.ss.android.ugc.aweme:id/content_layout"))
        )
        if home_button:
            home_button[-1].click()
            print("✅ 成功进入个人主页")
        else:
            print("❌ 未能找到主页按钮")
            return

        # 点击 "添加朋友" 按钮
        add_friend_button = WebDriverWait(drivers, 10).until(
            EC.element_to_be_clickable((By.ID, "com.ss.android.ugc.aweme:id/tv_desc"))
        )
        add_friend_button.click()
        print("✅ 进入添加朋友页面")

    except Exception as e:
        print(f"❌ 页面加载失败或出错: {e}")

#%%
#搜素抖音号并进入用户主页
def search_douyin_id(drivers,douyin_ids):
    try:
        #点击搜索框并输入抖音id
        search_box = WebDriverWait(drivers, 10).until(
            EC.element_to_be_clickable((By.ID, "com.ss.android.ugc.aweme:id/fpw"))
        )
        search_box.click()
        search_box.clear()
        search_box.send_keys(douyin_ids)  # 输入从数据库获取的抖音id
        print("✅ 已输入抖音id")

        #点击搜索按钮
        search_button = WebDriverWait(drivers, 10).until(
            EC.element_to_be_clickable((By.ID, "com.ss.android.ugc.aweme:id/3i="))
        )
        search_button.click()
        print("✅ 成功点击搜索按钮")

        #进入首个最匹配的用户主页
        user_list = WebDriverWait(drivers, 10).until(
            EC.presence_of_all_elements_located((By.ID, "com.ss.android.ugc.aweme:id/iq3"))
        )
        if user_list:
            user_list[0].click()
            print("✅ 进入目标用户主页")
        else:
            print("❌ 未找到目标用户")

    except Exception as e:
        print(f"❌ 操作失败: {e}")
#%%
#私信函数
def send_message(drivers):
    try:
        # 等待并点击“更多”按钮
        more_button = WebDriverWait(drivers, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@content-desc="更多"]'))
        )
        more_button.click()
        print("✅ 成功点击‘更多’按钮")

        # 等待并点击“发私信”按钮
        send_message_button = WebDriverWait(drivers, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@text="发私信"]'))
        )
        send_message_button.click()
        print("✅ 成功点击‘发私信’按钮")

        # 等待消息输入框可用，并输入消息
        message_input = WebDriverWait(drivers, 10).until(
            EC.presence_of_element_located((By.ID, "com.ss.android.ugc.aweme:id/msg_et"))
        )
        message_input.send_keys("你好")
        print("✅ 成功输入消息：你好")

        # 等待并点击“发送”按钮
        send_button = WebDriverWait(drivers, 10).until(
            EC.element_to_be_clickable((By.ID, "com.ss.android.ugc.aweme:id/jdf"))
        )
        send_button.click()
        print("✅ 成功发送消息")

    except Exception as e:
        print(f"❌ 操作失败: {e}")
#%%
#返回搜索界面
def return_to_search(drivers):
    try:
        for _ in range(3):  # 连续点击3次返回，确保回到搜索界面
            drivers.back()
            time.sleep(1)  # 等待1秒，确保页面加载
        print("✅ 已返回搜索界面")
    except Exception as e:
        print(f"❌ 返回搜索界面失败: {e}")
#%%
#主函数
# if __name__ == "__main__":
#     connection = database_connect.connect_to_db()#连接数据库
#     driver = webdriver.Remote("http://127.0.0.1:4723/wd/hub", options=config_settings())#启动参数配置
#     if connection:
#         try:
#             ensure_status_column(connection)  # 确保 status 列存在
#             init_and_open_home_page(driver)  # 进入添加朋友界面
#             while True:
#                 douyin_id = get_douyin_ids(connection)
#                 if douyin_id:
#                     search_douyin_id(driver, douyin_id)  # 搜索并进入用户主页
#                     send_message(driver)  # 发送私信
#                     update_douyin_status(connection, douyin_id)  # 更新数据库状态
#                     return_to_search(driver)  # 返回搜索界面，准备处理下一个抖音号
#                 else:
#                     print("✅ 所有抖音号已处理完毕")
#                     drop_status_column(connection)  # 处理完后删除 `status` 列
#                     break
#         finally:
#             connection.close()
#             driver.quit()
#     else:
#         print("❌ 无法连接到数据库")