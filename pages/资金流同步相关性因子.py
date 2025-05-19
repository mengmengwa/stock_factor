import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pymysql
from datetime import datetime, timedelta

# 1. 页面配置
st.set_page_config(layout="wide", page_title="资金流同步性因子", page_icon="💹")

# 2. 数据库连接管理（复用已有）
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

# 3. 数据获取函数
@st.cache_data(ttl=600)
def get_flow_data(index_name, days, start_date, end_date):
    """获取超大单和小单资金流数据并计算相关性"""
    try:
        conn = DBManager.get_connection()
        # 假设数据库中存在包含超大单和小单资金流的表
        query = f"""
        SELECT time, large_flow, small_flow 
        FROM `{index_name}_flow` 
        WHERE time BETWEEN %s AND %s
        ORDER BY time
        """
        df = pd.read_sql(query, conn, params=[start_date, end_date])
        
        if not df.empty:
            # 计算滚动相关性
            df['correlation'] = df['large_flow'].rolling(window=days).corr(df['small_flow'])
            # 计算分位数
            df['percentile'] = df['correlation'].rank(pct=True)
            # 计算相关系数变化率
            df['corr_change'] = df['correlation'].pct_change()
            
        return df.dropna()
    except pymysql.Error as e:
        st.error(f"数据库查询失败: {str(e)}")
        return pd.DataFrame()

# 4. 主函数
def main():
    st.title("💹 指数资金流同步性分析")
    
    try:
        # 侧边栏控件
        with st.sidebar:
            st.markdown("### 🛠️ 分析设置")
            days = st.slider("相关系数计算周期(天)", 5, 60, 20, help="用于计算资金流相关性的滚动窗口大小")
            date_range = st.date_input("分析时段", 
                value=[datetime.now() - timedelta(days=365), datetime.now()],
                help="选择要分析的历史数据范围")
            
            conn = DBManager.get_connection()
            # 获取所有可用的指数（假设表名格式为：指数名_flow）
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES LIKE '%_flow'")
            indices = [table[0].replace('_flow', '') for table in cursor.fetchall()]
            cursor.close()
            
            selected = st.multiselect("选择指数", indices, default=indices[:3], 
                                     help="选择要分析的指数，最多可同时分析3个")
            
            st.markdown("---")
            st.markdown("### 📖 因子说明")
            st.info("""
            **资金流同步性因子**衡量超大单（>100万元）和小单（<4万元）资金净流入的相关性：
            - 高同步性（相关系数接近1）表明大资金与散户行为一致，可能强化趋势
            - 低同步性（相关系数接近-1）表明大资金与散户行为背离，可能预示反转
            - 该因子的α收益来源于资金流强度与市场情绪的共同作用
            """)

        # 主分析逻辑
        if selected and len(date_range) == 2:
            results, corr_data = [], pd.DataFrame()
            
            # 进度条
            progress_bar = st.progress(0)
            total = len(selected)
            
            for i, idx in enumerate(selected):
                df = get_flow_data(idx, days, date_range[0], date_range[1])
                if not df.empty:
                    latest = df.iloc[-1]
                    results.append({
                        '指数': idx,
                        '相关系数': f"{latest['correlation']:.4f}",
                        '分位值': f"{latest['percentile']:.1%}",
                        '20日变化率': f"{latest['corr_change']:.2%}" if pd.notna(latest['corr_change']) else "N/A"
                    })
                    corr_data[idx] = df.set_index('time')['correlation']
                
                progress_bar.progress((i + 1) / total)
            
            progress_bar.empty()  # 分析完成后隐藏进度条

            # 显示结果
            if results:
                # 1. 相关性趋势图
                st.subheader("📊 资金流相关性趋势")
                fig = px.line(corr_data.reset_index().melt(id_vars='time'), 
                             x='time', y='value', color='variable',
                             labels={'value': '相关系数', 'time': '日期'},
                             title=f"超大单与小单资金流滚动{days}日相关性")
                fig.update_layout(
                    yaxis=dict(range=[-1, 1]),
                    height=450,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                # 添加参考线
                fig.add_hline(y=0, line_dash="dash", line_color="gray")
                fig.add_hline(y=0.8, line_dash="dot", line_color="red", annotation_text="强正相关")
                fig.add_hline(y=-0.8, line_dash="dot", line_color="blue", annotation_text="强负相关")
                st.plotly_chart(fig, use_container_width=True)
                
                # 2. 表格数据
                st.subheader("📈 最新因子数据")
                
                # 美化表格样式
                styled_df = pd.DataFrame(results).style\
                    .background_gradient(
                        subset=['相关系数'], 
                        cmap='coolwarm', 
                        vmin=-1, 
                        vmax=1
                    )\
                    .applymap(
                        lambda x: 'background-color: #ffcccc' if float(x.strip('%')) > 80 else 
                                  'background-color: #ccffcc' if float(x.strip('%')) < 20 else '',
                        subset=['分位值']
                    )\
                    .applymap(
                        lambda x: 'color: red; font-weight: bold' if isinstance(x, str) and float(x.strip('%')) > 0 else 
                                  'color: green; font-weight: bold' if isinstance(x, str) and float(x.strip('%')) < 0 else '',
                        subset=['20日变化率']
                    )
                
                st.dataframe(styled_df, hide_index=True)
                
                # 3. 分析见解
                st.subheader("💡 因子分析见解")
                for result in results:
                    index = result['指数']
                    corr = float(result['相关系数'])
                    pct = float(result['分位值'].strip('%'))
                    change = float(result['20日变化率'].strip('%')) if result['20日变化率'] != "N/A" else 0
                    
                    insights = []
                    
                    # 相关性强度分析
                    if corr > 0.7:
                        insights.append(f"- **强正相关 ({corr:.2f})**：超大单与小单资金流向高度一致，市场情绪统一，趋势可能持续。")
                    elif corr < -0.7:
                        insights.append(f"- **强负相关 ({corr:.2f})**：超大单与小单资金流向明显背离，大资金与散户行为分歧，可能预示趋势反转。")
                    else:
                        insights.append(f"- **弱相关 ({corr:.2f})**：超大单与小单资金流向关联性较弱，市场缺乏明确方向。")
                    
                    # 分位值分析
                    if pct > 80:
                        insights.append(f"- **历史高位 ({pct:.1f}%)**：当前相关系数处于历史较高水平，需警惕趋势动能衰减。")
                    elif pct < 20:
                        insights.append(f"- **历史低位 ({pct:.1f}%)**：当前相关系数处于历史较低水平，可能孕育反转机会。")
                    else:
                        insights.append(f"- **历史中位 ({pct:.1f}%)**：当前相关系数处于历史中等水平，市场情绪平稳。")
                    
                    # 变化率分析
                    if abs(change) > 20:
                        if change > 0:
                            insights.append(f"- **快速上升 ({change:.1f}%)**：相关系数近期快速上升，资金流向一致性增强，趋势可能加速。")
                        else:
                            insights.append(f"- **快速下降 ({change:.1f}%)**：相关系数近期快速下降，资金流向分歧加大，市场不确定性增加。")
                    
                    # 显示见解
                    with st.expander(f"📌 {index} 因子解读"):
                        for insight in insights:
                            st.markdown(insight)
                        st.markdown("---")
                        st.info("""
                        **投资建议参考**：
                        - 高正相关且上升趋势：顺势而为，持有或增持
                        - 高正相关但下降趋势：警惕回调，考虑减仓
                        - 高负相关且上升趋势：分歧加剧，观望为主
                        - 高负相关且下降趋势：关注反转信号，可能布局
                        """)
                        
    finally:
        DBManager.close_all()

if __name__ == "__main__":
    main()