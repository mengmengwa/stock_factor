import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import pymysql
from datetime import datetime, timedelta
import traceback

# 1. 页面配置
st.set_page_config(layout="wide", page_title="指数动量因子", page_icon="📊")

# 2. 数据库连接管理（使用SQLAlchemy）
class DBManager:
    _engines = {}  # 改为字典存储不同数据库的引擎
    
    @classmethod
    def get_engine(cls, database_name):
        """根据database_name创建并返回SQLAlchemy数据库引擎"""
        if database_name not in cls._engines:
            try:
                # 从secrets获取数据库配置
                db_config = st.secrets.mysql
                # 构建数据库连接URI
                db_uri = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{database_name}"
                # 创建数据库引擎
                cls._engines[database_name] = create_engine(
                    db_uri,
                    pool_size=5,
                    max_overflow=10,
                    pool_recycle=3600,
                    connect_args={"charset": "utf8mb4"}
                )
                st.info(f"数据库 {database_name} 连接成功")
            except Exception as e:
                st.error(f"数据库 {database_name} 连接配置错误: {str(e)}")
                st.error(traceback.format_exc())  # 显示详细错误堆栈
        return cls._engines.get(database_name)

# 3. 数据获取函数（使用SQLAlchemy引擎）
@st.cache_data(ttl=600)
def get_data(database_name, index_name, days, start_date, end_date):
    """获取指数数据并计算动量因子"""
    try:
        engine = DBManager.get_engine(database_name)
        if not engine:
            return pd.DataFrame()
            
        # 执行SQL查询
        query = f"""
        SELECT time, close 
        FROM `{index_name}` 
        WHERE time BETWEEN %s AND %s
        ORDER BY time
        """
        # 使用SQLAlchemy引擎连接执行查询，避免Pandas警告
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=(start_date, end_date)) 
        
        if not df.empty:
            # 计算动量因子和百分位
            df['momentum'] = df['close'].pct_change(days)
            df['percentile'] = df['momentum'].rank(pct=True)
        
        return df.dropna()
    
    except Exception as e:
        st.error(f"数据获取失败: {str(e)}")
        st.error(traceback.format_exc())  # 显示详细错误堆栈
        return pd.DataFrame()

# 4. 主函数
def main():
    st.title("📈 指数动量分析")
    
    # 设置当前页面使用的数据库名称
    database_name = "index_price_day"  # 修改为你的实际数据库名
    
    try:
        # 侧边栏控件
        with st.sidebar:
            st.markdown("### ⚙️ 分析参数设置")
            days = st.slider("计算周期(天)", 5, 60, 20, 
                            help="用于计算动量的历史交易日天数")
            date_range = st.date_input(
                "分析时段", 
                value=[datetime.now() - timedelta(days=365), datetime.now()],
                help="选择要分析的历史数据范围"
            )
            
            # 获取所有可用指数
            engine = DBManager.get_engine(database_name)
            if not engine:
                st.error("无法获取数据库连接")
                return
                
            with engine.connect() as conn:
                indices = pd.read_sql("SHOW TABLES", conn).iloc[:, 0].tolist()
            
            # 过滤表名，只保留指数表（根据实际表名规则调整）
            index_tables = [t for t in indices if not t.endswith('_flow')]
            selected = st.multiselect(
                "选择指数", 
                index_tables, 
                default=index_tables[:3],
                help="选择要分析的指数，最多可同时分析3个"
            )
        
        # 主分析逻辑
        if selected and len(date_range) == 2:
            results, momentum_data = [], pd.DataFrame()
            
            # 显示进度条
            progress_bar = st.progress(0)
            total_indices = len(selected)
            
            for i, idx in enumerate(selected):
                df = get_data(database_name, idx, days, date_range[0], date_range[1])
                if not df.empty:
                    latest = df.iloc[-1]
                    results.append({
                        '指数': idx,
                        '动量值': f"{latest['momentum']:.2%}",
                        '分位值': f"{latest['percentile']:.1%}"
                    })
                    momentum_data[idx] = df.set_index('time')['momentum']
                
                # 更新进度条
                progress_bar.progress((i + 1) / total_indices)
            
            # 分析完成后隐藏进度条
            progress_bar.empty()
            
            # 显示结果
            if results:
                # 动量因子走势图
                st.subheader("📊 指数动量因子走势图")
                fig = px.line(
                    momentum_data.reset_index().melt(id_vars='time'), 
                    x='time', 
                    y='value', 
                    color='variable',
                    labels={'value': '动量值', 'time': '日期'},
                    title=f'指数动量因子（{days}日周期）'
                )
                fig.update_layout(
                    height=500,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # 动量因子数据表格
                st.subheader("📋 最新动量因子数据")
                st.dataframe(
                    pd.DataFrame(results).style.applymap(
                        lambda x: 'background-color: #ffcccc' if float(x.strip('%')) > 80 else 
                                'background-color: #ccffcc' if float(x.strip('%')) < 20 else '',
                        subset=['分位值']
                    ), 
                    hide_index=True
                )
                
                # 分析见解
                st.subheader("💡 动量因子分析见解")
                for result in results:
                    index = result['指数']
                    momentum = float(result['动量值'].strip('%'))
                    percentile = float(result['分位值'].strip('%'))
                    
                    insights = []
                    
                    # 动量值分析
                    if momentum > 5:
                        insights.append(f"- **高动量 ({momentum:.1f}%)**：{index}近期上涨动能强劲，处于上升趋势。")
                    elif momentum < -5:
                        insights.append(f"- **低动量 ({momentum:.1f}%)**：{index}近期下跌动能明显，处于下降趋势。")
                    else:
                        insights.append(f"- **中性动量 ({momentum:.1f}%)**：{index}近期走势平稳，缺乏明确趋势。")
                    
                    # 分位值分析
                    if percentile > 80:
                        insights.append(f"- **历史高位 ({percentile:.1f}%)**：当前动量处于历史较高水平，可能面临回调风险。")
                    elif percentile < 20:
                        insights.append(f"- **历史低位 ({percentile:.1f}%)**：当前动量处于历史较低水平，可能孕育反弹机会。")
                    else:
                        insights.append(f"- **历史中位 ({percentile:.1f}%)**：当前动量处于历史中等水平，市场情绪平稳。")
                    
                    # 显示见解
                    with st.expander(f"📌 {index} 因子解读"):
                        for insight in insights:
                            st.markdown(insight)
                        st.markdown("---")
                        st.info("""
                        **投资建议参考**：
                        - 高动量且高百分位：谨慎追高，考虑获利了结
                        - 高动量但低百分位：趋势可能延续，可考虑增持
                        - 低动量且高百分位：可能反转，考虑减仓或做空
                        - 低动量但低百分位：可能超跌反弹，可关注底部信号
                        """)
    
    except Exception as e:
        st.error(f"应用运行出错: {str(e)}")
        st.error(traceback.format_exc())  # 显示详细错误堆栈

if __name__ == "__main__":
    main()