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

# 2. 数据库连接函数
@st.cache_resource(ttl=3600)  # 缓存数据库连接1小时
def connect_db():
    try:
        return pymysql.connect(**st.secrets.mysql)
    except Exception as e:
        st.error(f"数据库连接失败: {e}")
        return None

# 3. 数据获取函数
@st.cache_data(ttl=600)  # 缓存数据10分钟
def get_data(conn, index_name, start_date, end_date):
    query = f"""
    SELECT time, close 
    FROM `{index_name}` 
    WHERE time BETWEEN %s AND %s
    ORDER BY time
    """
    return pd.read_sql(query, conn, params=[start_date, end_date], parse_dates=['time'])

# 4. 动量计算函数
def calc_momentum(df, days=20):
    df['momentum'] = df['close'].pct_change(days)
    df['percentile'] = df['momentum'].rank(pct=True)
    return df.dropna()

# 5. 页面布局
def main():
    st.title("📈 指数动量分析仪表板")
    
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
        conn = connect_db()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            indices = [t[0] for t in cursor.fetchall()]
            selected = st.multiselect(
                "选择指数", 
                indices, 
                default=indices[:3],
                help="可多选，最多同时显示5个指数"
            )
        else:
            selected = []
    
    # 主显示区
    if selected and len(date_range) == 2:
        analyze_momentum(conn, selected, days, date_range[0], date_range[1])
    
    if conn:
        conn.close()

# 6. 动量分析函数
def analyze_momentum(conn, indices, days, start_date, end_date):
    # 计算动量数据
    momentum_data = pd.DataFrame()
    results = []
    
    for idx in indices:
        df = get_data(conn, idx, start_date, end_date)
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
        plot_df = momentum_data.reset_index().melt(id_vars='time')
        
        fig = px.line(
            plot_df, 
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
        
        # 显示分析结果
        show_results(results)

# 7. 结果显示函数
def show_results(results):
    st.subheader("当前动量分析")
    result_df = pd.DataFrame(results)
    result_df['动量值'] = result_df['动量值'].apply(lambda x: f"{x:.2%}")
    result_df['分位值'] = result_df['分位值'].apply(lambda x: f"{x:.1%}")
    
    # 颜色标记
    def color_percentile(val):
        val = float(val.strip('%'))
        if val > 80:
            return 'background-color: #ffcccc; color: #d62728'  # 红色高亮
        elif val < 20:
            return 'background-color: #ccffcc; color: #2ca02c'  # 绿色高亮
        return ''
    
    styled_df = result_df.style.applymap(
        color_percentile, 
        subset=['分位值']
    ).set_properties(**{
        'text-align': 'center',
        'font-size': '14px'
    })
    
    st.dataframe(
        styled_df,
        hide_index=True,
        use_container_width=True
    )
    
    # 下载按钮
    csv = result_df.to_csv(index=False, encoding='utf_8_sig')
    st.download_button(
        "📥 下载分析结果", 
        data=csv,
        file_name=f"动量分析_{datetime.now().strftime('%Y%m%d')}.csv",
        mime='text/csv',
        help="下载当前显示的分析结果"
    )

if __name__ == "__main__":
    main()