import streamlit as st
import pandas as pd
import plotly.express as px
import pymysql
from datetime import datetime, timedelta

import requests
import streamlit as st

# def get_public_ip():
#     try:
#         ip = requests.get('https://api.ipify.org').text
#         st.write(f"当前 Streamlit 公网 IP: `{ip}`")
#     except Exception as e:
#         st.error(f"获取 IP 失败: {e}")

# get_public_ip()


# 1. 数据库连接

def connect_db():
    return pymysql.connect(**st.secrets.mysql)

# 2. 数据获取
def get_data(conn, index_name):
    query = f"SELECT time, close FROM `{index_name}` ORDER BY time"
    return pd.read_sql(query, conn, parse_dates=['time'])

# 3. 动量计算
def calc_momentum(df, days=20):
    df['momentum'] = df['close'].pct_change(days)
    df['percentile'] = df['momentum'].rank(pct=True)
    return df.dropna()

# 4. 页面设置
st.set_page_config(layout="wide", page_title="指数动量分析")
st.title("📈 指数动量分析仪表板")

# 5. 侧边栏控件
with st.sidebar:
    st.header("参数设置")
    days = st.slider("计算周期(天)", 5, 60, 20)
    start_date = st.date_input("开始日期", datetime.now() - timedelta(days=365))
    end_date = st.date_input("结束日期", datetime.now())
    
    # 获取指数列表
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        indices = [t[0] for t in cursor.fetchall()]
        selected = st.multiselect("选择指数", indices, default=indices[:3])
    else:
        selected = []

# 6. 主显示区
if selected:
    # 计算动量数据
    momentum_data = pd.DataFrame()
    results = []
    
    for idx in selected:
        df = get_data(conn, idx)
        df = df[(df['time'] >= pd.to_datetime(start_date)) & 
               (df['time'] <= pd.to_datetime(end_date))]
        
        if not df.empty:
            df = calc_momentum(df, days)
            latest = df.iloc[-1]
            results.append({
                '指数': idx,
                '动量值': f"{latest['momentum']:.2%}",
                '分位值': f"{latest['percentile']:.1%}"
            })
            momentum_data[idx] = df.set_index('time')['momentum']
    
    # 7. Plotly交互图表
    if not momentum_data.empty:
        st.subheader("动量走势图")
        plot_df = momentum_data.reset_index().melt(id_vars='time')
        
        fig = px.line(plot_df, 
                     x='time', 
                     y='value', 
                     color='variable',
                     labels={'value': '动量值', 'time': '日期'},
                     height=500)
        
        # 添加参考线
        fig.add_hline(y=0, line_dash="dot", line_color="gray")
        st.plotly_chart(fig, use_container_width=True)
        
        # 8. 结果表格
        st.subheader("当前动量分析")
        result_df = pd.DataFrame(results)
        
        # 颜色标记
        def color_percentile(val):
            val = float(val.strip('%'))
            color = 'red' if val > 80 else 'green' if val < 20 else None
            return f"color: {color}" if color else ""
        
        st.dataframe(
            result_df.style.applymap(color_percentile, subset=['分位值']),
            hide_index=True,
            width=800
        )
        
        # 9. 下载按钮
        csv = result_df.to_csv(index=False, encoding='utf_8_sig')
        st.download_button(
            "📥 下载CSV", 
            data=csv,
            file_name=f"动量分析_{days}天.csv",
            mime='text/csv'
        )

if conn:
    conn.close()