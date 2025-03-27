import Dyin_sendM as dys
def process_single(driver, connection):
    """
    处理单个抖音 ID 的私信流程：
    1. 从数据库获取待处理的抖音 ID
    2. 搜索该抖音号并进入主页
    3. 发送私信
    4. 更新数据库状态
    5. 返回搜索页面，准备处理下一个
    """
    douyin_id = dys.get_douyin_ids(connection)
    if douyin_id:
        dys.search_douyin_id(driver, douyin_id)  # 搜索抖音号
        dys.send_message(driver)  # 发送私信
        dys.update_douyin_status(connection, douyin_id)  # 更新数据库状态
        dys.return_to_search(driver)  # 返回搜索界面
        return True  # 表示还有数据未处理
    else:
        print("✅ 所有抖音号已处理完毕")
        dys.drop_status_column(connection)  # 删除 `status` 列
        return False  # 表示所有抖音号都处理完了