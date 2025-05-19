import streamlit as st
import pandas as pd
from scipy import stats
from scipy.stats import norm
import numpy as np
from scipy.stats import spearmanr
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from sqlalchemy import create_engine
import traceback
import matplotlib as mpl

# 添加字体注册代码
font_path = 'fonts/SimHei.ttf'  # 字体文件路径 
mpl.font_manager.fontManager.addfont(font_path)  # 注册字体
plt.rcParams['font.sans-serif'] = ['SimHei']  # 指定字体名称
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 1. 页面配置
st.set_page_config(layout="wide", page_title="资金流同步相关性因子", page_icon="📊")

# 2. 数据库连接管理（支持多数据库）
class DBManager:
    _engines = {}  # 使用字典存储不同数据库的引擎
    
    @classmethod
    def get_engine(cls, database_name=None):
        """根据database_name创建并返回SQLAlchemy数据库引擎"""
        if database_name not in cls._engines:
            try:
                # 从secrets获取基础配置
                db_config = st.secrets.mysql
                # 构建数据库连接URI
                db_uri = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{database_name if database_name else ''}"
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
                st.error(f"数据库 {database_name} 连接错误: {str(e)}")
                st.error(traceback.format_exc())
        return cls._engines.get(database_name)

# 3. 数据获取函数（支持从不同数据库获取数据）
def get_data_from_db(database_name, table_name):
    """从指定数据库获取表数据"""
    engine = DBManager.get_engine(database_name)
    if not engine:
        return pd.DataFrame()
    
    query = f"SELECT * FROM `{table_name}`"
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"从数据库 {database_name} 获取表 {table_name} 数据失败: {str(e)}")
        return pd.DataFrame()

# 4. 计算资金流同步相关性因子
def calculate_factor(index_code):
    """计算因子值"""
    # 从不同数据库获取大单和小单资金流数据
    df_large = get_data_from_db('index_big_order', index_code)
    df_small = get_data_from_db('index_small_order', index_code)
    
    if df_large.empty or df_small.empty:
        st.warning(f"无法获取 {index_code} 的资金流数据，请检查数据库连接或表名")
        return pd.DataFrame()
    
    # 计算净流入（添加列存在性检查）
    required_columns = ['ths_active_buy_large_amt_hb_index', 'ths_active_sell_large_amt_hb_index',
                        'ths_active_buy_small_amt_index', 'ths_active_sell_small_amt_index']
    if not set(required_columns).issubset(df_large.columns.union(df_small.columns)):
        st.error("数据库表缺少必要列，请检查表结构")
        return pd.DataFrame()
    
    df_large['ELt'] = df_large['ths_active_buy_large_amt_hb_index'] - df_large['ths_active_sell_large_amt_hb_index']
    df_small['St'] = df_small['ths_active_buy_small_amt_index'] - df_small['ths_active_sell_small_amt_index']
  
    # 合并数据
    df = pd.merge(
        df_large[['time', 'thscode', 'ELt']], 
        df_small[['time', 'thscode', 'St']], 
        on=['time', 'thscode'],
        how='inner'
    )
    
    # 计算20日滚动秩相关系数
    def rolling_spearman(series1, series2, window):
        return series1.rolling(window).apply(
            lambda x: spearmanr(x, series2.loc[x.index])[0], 
            raw=False
        )
    df['RankCorr_ELt_St'] = rolling_spearman(df['ELt'], df['St'], window=20)
    return df.dropna()

# 5. 指数走势与因子对比绘图（显式传递figure）
def plot_index_factor_comparison(merged, index_code):
    """绘制双轴对比图"""
    plt.rcParams['axes.unicode_minus'] = False
    fig, ax1 = plt.subplots(figsize=(16, 7))  # 显式创建figure
    ax2 = ax1.twinx()
    
    # 绘制指数收盘价
    line1, = ax1.plot(merged['time'], merged['close'], color='#1f77b4', linewidth=2.5, label=f'{index_code}收盘价')
    ax1.set_ylabel('收盘价', fontsize=12)
    
    # 绘制因子值
    line2, = ax2.plot(merged['time'], merged['RankCorr_ELt_St'], color='#d62728', linestyle='--', linewidth=1.8, label='资金流同步性因子')
    ax2.set_ylabel('RankCorr(ELt,St)', fontsize=12)
    ax2.axhline(y=0, color='gray', linestyle=':')
    
    # 合并图例
    handles = [line1, line2]
    labels = [line1.get_label(), line2.get_label()]
    ax1.legend(handles, labels, loc='upper left', bbox_to_anchor=(0, 1, 1, 0.2), ncol=2)
    
    # 优化坐标轴
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.xticks(rotation=45)
    plt.title(f'{index_code}走势与资金流同步性因子', pad=20)
    plt.tight_layout()  # 优化布局
    st.pyplot(fig)  # 传递figure对象

# 6. 因子分布绘图（显式传递figure）
def plot_factor_distribution(factor_values, index_code):
    """绘制因子值分布直方图"""
    if factor_values.empty:
        st.warning("因子值为空，无法绘制分布图")
        return
    
    fig, ax = plt.subplots(figsize=(12, 6))  # 显式创建figure
    sns.histplot(factor_values, bins=40, kde=True, color='#d62728', alpha=0.5, edgecolor='w', ax=ax)
    
    # 计算统计量
    mean_val = np.mean(factor_values)
    median_val = np.median(factor_values)
    std_val = np.std(factor_values)
    
    # 正态分布曲线
    x = np.linspace(min(factor_values), max(factor_values), 100)
    normal_curve = stats.norm.pdf(x, loc=mean_val, scale=std_val) * len(factor_values) * (max(factor_values)-min(factor_values))/40
    ax.plot(x, normal_curve, 'r--', linewidth=1.5, label='正态分布')
    
    # 添加统计标注
    ax.text(0.02, 0.95, 
            f'均值: {mean_val:.2f}\n中位数: {median_val:.2f}\n标准差: {std_val:.2f}',
            transform=ax.transAxes, ha='left', va='top',
            bbox=dict(facecolor='white', alpha=0.8))
    
    # 分位线
    for q in [0.1, 0.25, 0.75, 0.9]:
        q_val = np.quantile(factor_values, q)
        ax.axvline(x=q_val, color='#7f7f7f', linestyle='--', alpha=0.5)
        ax.text(q_val, ax.get_ylim()[1]*0.8, f'{int(q*100)}%', rotation=90, va='top', ha='right')
    
    # 峰度/偏度
    kurtosis = stats.kurtosis(factor_values)
    skewness = stats.skew(factor_values)
    ax.text(0.02, 0.75, 
            f'峰度: {kurtosis:.2f}\n偏度: {skewness:.2f}',
            transform=ax.transAxes, ha='left', va='top',
            bbox=dict(facecolor='white', alpha=0.8))
    
    sns.despine(left=True)
    ax.set_title(f"{index_code}资金流同步性因子分布特征", pad=20)
    ax.set_xlabel('因子值', fontweight='bold')
    ax.set_ylabel('密度', fontweight='bold')
    ax.legend()
    plt.tight_layout()
    st.pyplot(fig)  # 传递figure对象

# 7. IC值计算与绘图（显式传递figure）
def plot_ic_values(index_code):
    """计算不同周期IC值并绘图"""
    holding_periods = [2, 3, 4, 5, 20]
    df_factor = calculate_factor(index_code)
    
    # 获取指数价格数据（从index_price_day数据库）
    df_index = get_data_from_db('index_price_day', index_code)
    if df_index.empty:
        st.warning(f"无法获取 {index_code} 的价格数据")
        return
    
    df_index['return'] = np.log(df_index['close']).pct_change()
    
    # 合并因子与收益率数据
    merged = pd.merge(
        df_factor[['time', 'RankCorr_ELt_St']].rename(columns={'RankCorr_ELt_St': 'factor_value'}),
        df_index[['time', 'return']],
        on='time',
        how='inner'
    ).dropna()
    
    if merged.empty:
        st.warning("因子与收益率数据合并后为空")
        return
    
    merged['time'] = pd.to_datetime(merged['time'])
    merged.set_index('time', inplace=True)
    
    ic_results = []
    for hold_days in holding_periods:
        # 计算累计收益率
        merged[f'return_{hold_days}d'] = merged['return'].shift(-1).rolling(hold_days).sum()
        
        # 计算IC值
        ic_values = merged.groupby(pd.Grouper(freq=f'{hold_days}D'))[
            ['factor_value', f'return_{hold_days}d']
        ].apply(lambda x: spearmanr(x['factor_value'], x[f'return_{hold_days}d'])[0] if len(x) >= 2 else np.nan)
        
        # 整理结果
        period_data = pd.DataFrame({
            'period': f'{hold_days}日',
            'date': ic_values.index,
            'ic': ic_values.values
        }).dropna()
        if not period_data.empty:
            period_data['cumulative_ic'] = period_data['ic'].cumsum()
            ic_results.append(period_data)
    
    if not ic_results:
        st.warning("未获取到IC计算结果")
        return
    
    ic_results = pd.concat(ic_results, ignore_index=False)
    
    # 统计摘要
    ic_summary = ic_results.groupby('period').agg({
        'ic': ['mean', 'std', lambda x: x.mean()/x.std() if x.std()!=0 else np.nan]
    }).rename(columns={'<lambda>': 'ic_ir'})
    ic_summary.columns = ['平均IC', 'IC标准差', 'IC_IR']
    st.write("不同周期IC值统计：")
    st.table(ic_summary)
    
    # 绘制子图
    n_periods = len(holding_periods)
    fig, axes = plt.subplots(n_periods, 2, figsize=(20, 4 * n_periods))  # 显式创建figure
    
    for i, hold_days in enumerate(holding_periods):
        period_data = ic_results[ic_results['period'] == f'{hold_days}日']
        if period_data.empty:
            continue
        
        # IC值走势
        ax_left = axes[i, 0]
        sns.lineplot(data=period_data, x='date', y='ic', ax=ax_left, marker='o', color='#1f77b4')
        ax_left.axhline(0, color='gray', linestyle='--')
        ax_left.set_title(f'{hold_days}日周期IC值', pad=10)
        ax_left.set_ylabel('IC值')
        ax_left.tick_params(axis='x', rotation=45)
        ax_left.text(0.95, 0.95, 
                     f'平均IC: {ic_summary.loc[f"{hold_days}日", "平均IC"]:.4f}\nIC_IR: {ic_summary.loc[f"{hold_days}日", "IC_IR"]:.2f}',
                     transform=ax_left.transAxes, ha='right', va='top',
                     bbox=dict(facecolor='white', alpha=0.8))
        
        # 累计IC走势
        ax_right = axes[i, 1]
        sns.lineplot(data=period_data, x='date', y='cumulative_ic', ax=ax_right, marker='o', color='#d62728')
        ax_right.axhline(0, color='gray', linestyle='--')
        ax_right.set_title(f'{hold_days}日周期累计IC', pad=10)
        ax_right.set_xlabel('时间')
        ax_right.tick_params(axis='x', rotation=45)
        ax_right.text(0.95, 0.95, 
                     f'累计IC: {period_data["cumulative_ic"].iloc[-1]:.4f}',
                     transform=ax_right.transAxes, ha='right', va='top',
                     bbox=dict(facecolor='white', alpha=0.8))
    
    plt.suptitle(f'{index_code}资金流因子IC分析', y=1.02, fontsize=14)
    plt.tight_layout()
    st.pyplot(fig)  # 传递figure对象

def main():
    st.title("📈 资金流同步相关性因子分析")
    try:
        # 侧边栏选择指数
        with st.sidebar:
            st.markdown("### ⚙️ 分析参数设置")
            
            # 从大单资金流数据库获取可用的指数列表
            engine = DBManager.get_engine('index_big_order')
            if not engine:
                st.error("无法连接到大单资金流数据库")
                return
                
            with engine.connect() as conn:
                indices = pd.read_sql("SHOW TABLES", conn).iloc[:, 0].tolist()
            
            selected_index = st.selectbox(
                "选择指数", 
                indices, 
                help="选择要分析的指数（表名对应指数代码）"
            )
        
        # 核心分析流程
        df_factor = calculate_factor(selected_index)
        if df_factor.empty:
            st.warning("未获取到因子数据，请检查指数选择或数据库连接")
            return
        
        # 获取指数价格数据（从index_price_day数据库）
        df_index = get_data_from_db('index_price_day', selected_index)
        if df_index.empty:
            st.warning(f"无法获取 {selected_index} 的价格数据")
            return
            
        merged = pd.merge(
            df_index[['time', 'close']], 
            df_factor[['time', 'RankCorr_ELt_St']], 
            on='time', 
            how='inner'
        )
        if merged.empty:
            st.warning("合并后数据为空，请检查时间范围")
            return
        
        # 绘制对比图
        st.subheader("📊 指数走势与因子值对比")
        plot_index_factor_comparison(merged, selected_index)
        
        # 绘制分布直方图
        st.subheader("📊 因子值统计分布")
        plot_factor_distribution(df_factor['RankCorr_ELt_St'].dropna(), selected_index)
        
        # 绘制IC值分析
        st.subheader("📊 因子预测能力（IC值分析）")
        plot_ic_values(selected_index)
        
    except Exception as e:
        st.error(f"应用运行出错: {str(e)}")
        st.error(traceback.format_exc())

if __name__ == "__main__":
    main()