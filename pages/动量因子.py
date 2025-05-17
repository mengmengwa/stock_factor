import streamlit as st
import pandas as pd
import plotly.express as px
import pymysql
from datetime import datetime, timedelta

# 1. 页面配置
st.set_page_config(layout="wide", page_title="指数动量分析", page_icon="📊")

# 2. 数据库连接管理（完全避免重复关闭）
class DBManager:
    _conn = None
    
    @classmethod
    def get_connection(cls):
        if cls._conn is None or cls._conn._closed:
            cls._conn = pymysql.connect(**st.secrets.mysql)
        return cls._conn
    
    @classmethod
    def close_all(cls):
        if cls._conn and not cls._conn._closed:
            cls._conn.close()

# 3. 数据获取函数（安全版本）
@st.cache_data(ttl=600)
def get_data(index_name, days, start_date, end_date):
    try:
        conn = DBManager.get_connection()
        query = f"""
        SELECT time, close 
        FROM `{index_name}` 
        WHERE time BETWEEN %s AND %s
        ORDER BY time
        """
        df = pd.read_sql(query, conn, params=[start_date, end_date])
        if not df.empty:
            df['momentum'] = df['close'].pct_change(days)
            df['percentile'] = df['momentum'].rank(pct=True)
        return df.dropna()
    except pymysql.Error as e:
        st.error(f"数据库查询失败: {str(e)}")
        return pd.DataFrame()

# 4. 主函数
def main():
    st.title("📈 指数动量分析")
    
    try:
        # 侧边栏控件
        with st.sidebar:
            days = st.slider("计算周期(天)", 5, 60, 20)
            date_range = st.date_input("分析时段", 
                value=[datetime.now() - timedelta(days=365), datetime.now()])
            
            conn = DBManager.get_connection()
            indices = pd.read_sql("SHOW TABLES", conn).iloc[:, 0].tolist()
            selected = st.multiselect("选择指数", indices, default=indices[:3])

        # 主分析逻辑
        if selected and len(date_range) == 2:
            results, momentum_data = [], pd.DataFrame()
            
            for idx in selected:
                df = get_data(idx, days, date_range[0], date_range[1])
                if not df.empty:
                    latest = df.iloc[-1]
                    results.append({
                        '指数': idx,
                        '动量值': f"{latest['momentum']:.2%}",
                        '分位值': f"{latest['percentile']:.1%}"
                    })
                    momentum_data[idx] = df.set_index('time')['momentum']

            # 显示结果
            if results:
                fig = px.line(momentum_data.reset_index().melt(id_vars='time'), 
                            x='time', y='value', color='variable',
                            labels={'value': '动量值', 'time': '日期'})
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(
                    pd.DataFrame(results).style.applymap(
                        lambda x: 'background-color: #ffcccc' if float(x.strip('%')) > 80 else 
                                'background-color: #ccffcc' if float(x.strip('%')) < 20 else '',
                        subset=['分位值']
                    ), 
                    hide_index=True
                )
    finally:
        DBManager.close_all()

if __name__ == "__main__":
    main()