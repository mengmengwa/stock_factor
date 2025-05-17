import streamlit as st
import pandas as pd
import plotly.express as px
import pymysql
from datetime import datetime, timedelta

# 1. 页面配置
st.set_page_config(
    layout="wide", 
    page_title="指数动量分析",
    page_icon="📊"
)

# 2. 数据库连接函数（使用单例模式）
_connection = None

def get_db_connection():
    """获取数据库连接（单例模式）"""
    global _connection
    if _connection is None or _connection._closed:
        _connection = pymysql.connect(**st.secrets.mysql)
    return _connection

# 3. 数据获取函数（不再自动关闭连接）
@st.cache_data
def get_data(index_name, start_date, end_date):
    conn = get_db_connection()
    query = """
    SELECT time, close 
    FROM `%s` 
    WHERE time BETWEEN %s AND %s
    ORDER BY time
    """
    return pd.read_sql(query, conn, params=[index_name, start_date, end_date])

# 4. 动量计算函数
def calc_momentum(df, days=20):
    df['momentum'] = df['close'].pct_change(days)
    df['percentile'] = df['momentum'].rank(pct=True)
    return df.dropna()

# 5. 页面布局
def main():
    st.title("📈 指数动量分析仪表板")
    
    try:
        # 侧边栏控件
        with st.sidebar:
            st.header("参数设置")
            days = st.slider("计算周期(天)", 5, 60, 20, help="计算动量的时间窗口")
            date_range = st.date_input(
                "分析时段",
                value=[datetime.now() - timedelta(days=365), datetime.now()],
                max_value=datetime.now()
            )
            
            # 获取指数列表
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            indices = [t[0] for t in cursor.fetchall()]
            selected = st.multiselect(
                "选择指数", 
                indices, 
                default=indices[:3],
                help="可多选，最多同时显示5个指数"
            )
        
        # 主显示区
        if selected and len(date_range) == 2:
            analyze_momentum(selected, days, date_range[0], date_range[1])
    
    finally:
        # 在应用退出时关闭连接
        if '_connection' in globals() and _connection is not None:
            _connection.close()

# ...（保持analyze_momentum和show_results函数不变）...

if __name__ == "__main__":
    main()