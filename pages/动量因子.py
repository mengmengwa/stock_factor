import streamlit as st
import pandas as pd
import plotly.express as px
import pymysql
from datetime import datetime, timedelta

# 1. 页面配置
st.set_page_config(layout="wide", page_title="指数动量分析", page_icon="📊")

# 2. 数据库连接（使用Streamlit缓存）
@st.cache_resource
def get_conn():
    return pymysql.connect(**st.secrets.mysql)

# 3. 数据获取函数
@st.cache_data(ttl=600)
def get_data(index_name, days, start_date, end_date):
    with get_conn() as conn:
        query = f"""
        SELECT time, close 
        FROM `{index_name}` 
        WHERE time BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY time
        """
        df = pd.read_sql(query, conn)
        if not df.empty:
            df['momentum'] = df['close'].pct_change(days)
            df['percentile'] = df['momentum'].rank(pct=True)
        return df.dropna()

# 4. 主函数
def main():
    st.title("📈 指数动量分析")
    
    # 侧边栏控件
    with st.sidebar:
        days = st.slider("计算周期(天)", 5, 60, 20)
        date_range = st.date_input("分析时段", 
            value=[datetime.now() - timedelta(days=365), datetime.now()])
        
        with get_conn() as conn:
            indices = pd.read_sql("SHOW TABLES", conn).iloc[:, 0].tolist()
            selected = st.multiselect("选择指数", indices, default=indices[:3])

    # 主分析逻辑
    if selected and len(date_range) == 2:
        results, momentum_data = [], pd.DataFrame()
        
        for idx in selected:
            df = get_data(idx, days, date_range[0], date_range[1])
            if not df.empty:
                latest = df.iloc[-1]
                results.append({'指数': idx, '动量值': latest['momentum'], '分位值': latest['percentile']})
                momentum_data[idx] = df.set_index('time')['momentum']

        # 显示结果
        if results:
            # 显示图表
            fig = px.line(momentum_data.reset_index().melt(id_vars='time'), 
                         x='time', y='value', color='variable',
                         labels={'value': '动量值', 'time': '日期'})
            st.plotly_chart(fig, use_container_width=True)
            
            # 显示表格
            result_df = pd.DataFrame(results)
            result_df['动量值'] = result_df['动量值'].apply(lambda x: f"{x:.2%}")
            result_df['分位值'] = result_df['分位值'].apply(lambda x: f"{x:.1%}")
            st.dataframe(result_df.style.applymap(
                lambda x: 'background-color: #ffcccc' if float(x.strip('%')) > 80 else 
                         'background-color: #ccffcc' if float(x.strip('%')) < 20 else '',
                subset=['分位值']
            ), hide_index=True)

if __name__ == "__main__":
    main()