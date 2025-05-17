import streamlit as st
import requests
from datetime import datetime, timedelta

# 放在最前面！
st.set_page_config(
    page_title="因子库分析平台",
    layout="wide",
    page_icon="📊"
)

# -------------------------------
# 新增：获取公网IP的模块
# -------------------------------
def get_public_ip():
    try:
        response = requests.get('https://api.ipify.org?format=json', timeout=5)
        if response.status_code == 200:
            return response.json()['ip']
        return "获取失败"
    except Exception as e:
        st.error(f"获取IP时出错: {e}")
        return None

# 在侧边栏显示IP信息
with st.sidebar:
    st.header("服务器信息")
    if st.button("点击获取公网IP"):
        ip = get_public_ip()
        if ip:
            st.success(f"当前公网IP: `{ip}`")
            st.info("请将此IP添加到数据库白名单")
        else:
            st.error("无法获取公网IP")

# -------------------------------
# 原有欢迎页内容
# -------------------------------
st.title("🎯 欢迎来到量化因子库！")
st.sidebar.success("👉 从侧边栏选择因子")

st.markdown("""
    ### 这是什么？
    本平台用于展示和分析量化因子，包括：
    - **估值因子**：PE、PB 等  
    - **动量因子**：短期动量、长期反转  
    - （更多因子正在开发...）

    ### 如何使用？
    1. 从左侧选择因子类型  
    2. 查看因子历史表现  
    3. 下载分析结果  
    """)