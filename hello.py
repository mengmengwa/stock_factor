import streamlit as st

# 放在最前面！
st.set_page_config(
    page_title="因子库分析平台",
    layout="wide",
    page_icon="📊"
)

# 欢迎页内容
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