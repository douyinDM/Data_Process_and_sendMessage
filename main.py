import comment_process as cp
import match
import database_connect as dbc
import Dyin_sendM as dys
from appium import webdriver
import single_send as sgs

if __name__ == "__main__":
    # 数据获取配置
    table_names_comment = "douyin_aweme_comment"
    table_names_video = "douyin_aweme"

    # 执行数据处理流程
    output_df = cp.process_comments(table_names_comment)
    match.match_and_save_data(output_df,table_names_video)

    connection = dbc.connect_to_db()#连接数据库
    driver = webdriver.Remote("http://127.0.0.1:4723/wd/hub", options=dys.config_settings())#启动参数配置
    if connection:
        try:
            dys.ensure_status_column(connection)  # 确保 status 列存在
            dys.init_and_open_home_page(driver)  # 进入添加朋友界面
            while True:
                sgs.process_single(driver,connection)
        finally:
            connection.close()
            driver.quit()
    else:
        print("❌ 无法连接到数据库")