import pandas as pd
import numpy as np
import re
import os
import logging
from datetime import datetime
import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# 配置日志
logging.basicConfig(
    filename='market_monitor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MarketMonitor:
    def __init__(self, report_file='analysis_report.md', output_file='market_monitor_report.md'):
        self.report_file = report_file
        self.output_file = output_file
        self.fund_codes = []
        self.fund_data = {}
        self.driver = None

    def _parse_report(self):
        """从 analysis_report.md 提取推荐基金代码"""
        logger.info("正在解析 %s 获取推荐基金代码...", self.report_file)
        if not os.path.exists(self.report_file):
            logger.error("报告文件 %s 不存在", self.report_file)
            raise FileNotFoundError(f"{self.report_file} 不存在")
        
        try:
            with open(self.report_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 提取推荐基金表格
            pattern = r'\| *(\d{6}) *\|.*?\| *(\d+\\.?\\d*) *\|'
            matches = re.findall(pattern, content)
            self.fund_codes = [code for code, _ in matches]
            logger.info("提取到 %d 个推荐基金: %s", len(self.fund_codes), self.fund_codes)
            
        except Exception as e:
            logger.error("解析报告文件失败: %s", e)
            raise

    def _get_fund_data_from_dayfund(self, fund_code):
        """使用 Selenium 从 dayfund.cn 抓取基金历史净值数据"""
        logger.info("正在获取基金 %s 的净值数据...", fund_code)
        
        try:
            # 配置 Chrome 选项，支持无头模式
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36')
            
            # 使用 webdriver-manager 自动管理 ChromeDriver
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            
            # 使用 HTTPS 协议
            url = f"https://www.dayfund.cn/fundvalue/{fund_code}.html"
            self.driver.get(url)
            logger.info("访问URL: %s", url)

            # 显式等待，确保净值表格加载完成
            wait = WebDriverWait(self.driver, 30)  # 增加超时到30秒
            table_element = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
            logger.info("表格元素加载完成")
            
            # 使用 pandas 读取表格
            df_list = pd.read_html(self.driver.page_source, flavor='html5lib')
            if not df_list:
                raise ValueError("未找到任何表格")
            
            # 查找包含净值数据的表格
            df = None
            for temp_df in df_list:
                if len(temp_df.columns) >= 9 and ('净值日期' in temp_df.columns or '日期' in str(temp_df.columns[0])):
                    df = temp_df
                    break
            
            if df is None:
                raise ValueError("未找到有效的净值表格")
            
            # 重命名列以匹配文档结构
            df.columns = ['date', 'fund_code', 'fund_name', 'net_value', 'accumulated_net_value', 
                          'prev_net_value', 'prev_accumulated_net_value', 'daily_growth_value', 'daily_growth_rate']
            
            # 数据清洗
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['net_value'] = pd.to_numeric(df['net_value'], errors='coerce')
            df = df.dropna(subset=['date', 'net_value'])
            df = df.drop_duplicates(subset=['date']).sort_values(by='date', ascending=True)
            
            if df.empty:
                raise ValueError("清洗后数据为空")
            
            logger.info("成功解析基金 %s 的数据，行数: %d, 最新日期: %s, 最新净值: %.4f", 
                        fund_code, len(df), df['date'].iloc[-1], df['net_value'].iloc[-1])
            return df[['date', 'net_value']]

        except Exception as e:
            logger.error("Selenium 抓取基金 %s 失败: %s", fund_code, str(e))
            if self.driver:
                with open(f"error_page_{fund_code}.html", "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source[:2000])  # 保存前2000字符
                logger.info("错误页面已保存到 error_page_%s.html", fund_code)
            return None
        finally:
            if self.driver:
                self.driver.quit()
                self.driver = None

    def get_fund_data(self):
        """获取所有基金的数据"""
        logger.info("开始获取 %d 个基金的数据...", len(self.fund_codes))
        for i, fund_code in enumerate(self.fund_codes, 1):
            logger.info("处理第 %d/%d 个基金: %s", i, len(self.fund_codes), fund_code)
            df = self._get_fund_data_from_dayfund(fund_code)
            
            if df is not None and not df.empty and len(df) >= 50:
                df = df.sort_values(by='date', ascending=True)
                # 计算 RSI (14日)
                delta = df['net_value'].diff()
                gain = delta.where(delta > 0, 0)
                loss = -delta.where(delta < 0, 0)
                avg_gain = gain.rolling(window=14).mean()
                avg_loss = loss.rolling(window=14).mean()
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
                
                # 计算 MA50
                ma50 = df['net_value'].rolling(window=50).mean()
                
                # 获取最新数据
                latest_data = df.iloc[-1]
                latest_net_value = latest_data['net_value']
                latest_rsi = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else float('nan')
                latest_ma50 = ma50.iloc[-1]
                latest_ma50_ratio = latest_net_value / latest_ma50 if not pd.isna(latest_ma50) and latest_ma50 != 0 else float('nan')

                self.fund_data[fund_code] = {
                    'latest_net_value': latest_net_value,
                    'rsi': latest_rsi,
                    'ma_ratio': latest_ma50_ratio
                }
                logger.info("成功计算基金 %s 的技术指标: 净值=%.4f, RSI=%.2f, MA50比率=%.2f", 
                            fund_code, latest_net_value, latest_rsi, latest_ma50_ratio)
            else:
                self.fund_data[fund_code] = None
                logger.warning("基金 %s 数据获取失败或数据不足，跳过计算 (数据行数: %s)", fund_code, len(df) if df is not None else 0)
            
            # 随机延迟，避免被网站限流
            time.sleep(random.uniform(1, 3))

    def generate_report(self):
        """生成市场情绪与技术指标监控报告"""
        logger.info("正在生成市场监控报告...")
        with open(self.output_file, 'w', encoding='utf-8') as f:
            f.write(f"# 市场情绪与技术指标监控报告\n\n")
            f.write(f"生成日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"## 推荐基金技术指标\n")
            f.write("| 基金代码 | 最新净值 | RSI | 净值/MA50 | 投资建议 |\n")
            f.write("|----------|----------|-----|-----------|----------|\n")
            
            for fund_code in self.fund_codes:
                if fund_code in self.fund_data and self.fund_data[fund_code]:
                    data = self.fund_data[fund_code]
                    rsi = data['rsi']
                    ma_ratio = data['ma_ratio']
                    advice = (
                        "等待回调" if not pd.isna(rsi) and rsi > 70 or not pd.isna(ma_ratio) and ma_ratio > 1.2 else
                        "可分批买入" if (pd.isna(rsi) or 30 <= rsi <= 70) and (pd.isna(ma_ratio) or 0.8 <= ma_ratio <= 1.2) else
                        "可加仓" if not pd.isna(rsi) and rsi < 30 else "观察"
                    )
                    f.write(f"| {fund_code} | {data['latest_net_value']:.4f} | {rsi:.2f if not pd.isna(rsi) else 'N/A'} | {ma_ratio:.2f if not pd.isna(ma_ratio) else 'N/A'} | {advice} |\n")
                else:
                    f.write(f"| {fund_code} | 数据获取失败 | - | - | 观察 |\n")
        
        logger.info("报告生成完成: %s", self.output_file)

if __name__ == "__main__":
    try:
        monitor = MarketMonitor()
        monitor._parse_report()
        monitor.get_fund_data()
        monitor.generate_report()
        logger.info("脚本执行完成")
    except Exception as e:
        logger.error("脚本运行失败: %s", e)
        with open('market_monitor.log', 'a', encoding='utf-8') as f:
            f.write(f"\n[CRITICAL] 脚本因致命错误终止: {e}\n")
