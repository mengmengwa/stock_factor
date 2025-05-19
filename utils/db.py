# utils/db.py
import streamlit as st
from sqlalchemy import create_engine
import traceback

class DBManager:
    _engines = {}  # 缓存不同数据库的引擎

    @classmethod
    def get_engine(cls, database_name):
        """根据 database_name 动态创建或返回引擎"""
        if database_name not in cls._engines:
            try:
                # 从 secrets 获取公共配置
                db_config = st.secrets.mysql
                
                # 动态构建连接字符串
                db_uri = (
                    f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
                    f"@{db_config['host']}:{db_config['port']}/{database_name}"
                )
                
                # 创建引擎并缓存
                cls._engines[database_name] = create_engine(
                    db_uri,
                    pool_size=5,
                    pool_recycle=3600,
                    connect_args={"charset": "utf8mb4"}
                )
                st.success(f"✅ 数据库 {database_name} 连接成功")
            except Exception as e:
                st.error(f"❌ 连接数据库 {database_name} 失败: {str(e)}")
                st.error(traceback.format_exc())
        
        return cls._engines.get(database_name)