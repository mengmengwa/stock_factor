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

# 2. 数据库连接管理（推荐Streamlit专用方式）
@st.cache_resource
def get_db_connection():
    """获取数据库连接（Streamlit推荐方式）"""
    return pymysql.connect(**st.secrets.mysql)

# 3. 数据获取函数
@st.cache_data
def get_data(index_name, start_date, end_date):
    conn = get_db_connection()
    try:
        query = """
        SELECT time, close 
        FROM `%s` 
        WHERE time BETWEEN %s AND %s
        ORDER BY time
        """
        return pd.read_sql(query, conn, params=[index_name, start_date, end_date])
    finally:
        conn.close()

# 4. 动量计算函数
def calc_momentum(df, days=20):
    df['momentum'] = df['close'].pct_change(days)
    df['percentile'] = df['momentum'].rank(pct=True)
    return df.dropna()

# 5. 分析结果显示函数
def show_results(results):
    st.subheader("当前动量分析")
    result_df = pd.DataFrame(results)
    result_df['动量值'] = result_df['动量值'].apply(lambda x: f"{x:.2%}")
    result_df['分位值'] = result_df['分位值'].apply(lambda x: f"{x:.1%}")
    
    # 颜色标记
    def color_percentile(val):
        val = float(val.strip('%'))
        if val > 80:
            return 'background-color: #ffcccc; color: #d62728'
        elif val < 20:
            return 'background-color: #ccffcc; color: #2ca02c'
        return ''
    
    st.dataframe(
        result_df.style.applymap(color_percentile, subset=['分位值']),
        hide_index=True,
        use_container_width=True
    )
    
    # 下载按钮
    st.download_button(
        "📥 下载分析结果", 
        data=result_df.to_csv(index=False, encoding='utf_8_sig'),
        file_name=f"动量分析_{datetime.now().strftime('%Y%m%d')}.csv",
        mime='text/csv'
    )

# 6. 动量分析核心函数
def analyze_momentum(indices, days, start_date, end_date):
    momentum_data = pd.DataFrame()
    results = []
    
    for idx in indices:
        df = get_data(idx, start_date, end_date)
        if not df.empty:
            df = calc_momentum(df, days)
            latest = df.iloc[-1]
            results.append({
                '指数': idx,
                '动量值': latest['momentum'],
                '分位值': latest['percentile']
            })
            momentum_data[idx] = df.set_index('time')['momentum']
    
    # 显示图表
    if not momentum_data.empty:
        st.subheader("动量走势图")
        fig = px.line(
            momentum_data.reset_index().melt(id_vars='time'),
            x='time',
            y='value',
            color='variable',
            labels={'value': '动量值', 'time': '日期'},
            height=500
        )
        fig.update_layout(
            hovermode="x unified",
            legend_title_text="指数名称"
        )
        fig.add_hline(y=0, line_dash="dot", line_color="gray")
        st.plotly_chart(fig, use_container_width=True)
        
        show_results(results)

# 7. 主函数
def main():
    st.title("📈 指数动量分析仪表板")
    
    with st.sidebar:
        st.header("参数设置")
        days = st.slider("计算周期(天)", 5, 60, 20)
        date_range = st.date_input(
            "分析时段",
            value=[datetime.now() - timedelta(days=365), datetime.now()],
            max_value=datetime.now()
        )
        
        # 获取指数列表
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            indices = [t[0] for t in cursor.fetchall()]
            selected = st.multiselect("选择指数", indices, default=indices[:3])
    
    if selected and len(date_range) == 2:
        analyze_momentum(selected, days, date_range[0], date_range[1])

if __name__ == "__main__":
    main()