import pandas as pd
import numpy as np
import re
import os
import logging
from datetime import datetime
import time
import random
from io import StringIO
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException
import tenacity

# 配置日志：使用 FileHandler 确保实时写入
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('market_monitor.log', encoding='utf-8'),
        logging.StreamHandler()  # 实时输出到控制台
    ]
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
            logger.info("analysis_report.md 内容（前1000字符）: %s", content[:1000])
            
            pattern = r'\| *(\d{6}) *\|.*?\|'
            matches = re.findall(pattern, content, re.MULTILINE)
            self.fund_codes = list(set(matches))  # 去重
            # 限制前10个基金用于测试，生产时移除
            self.fund_codes = self.fund_codes[:10]
            if not self.fund_codes:
                logger.warning("未提取到任何基金代码，请检查 analysis_report.md 是否包含基金代码表格")
            else:
                logger.info("提取到 %d 个推荐基金（测试限制前10个）: %s", len(self.fund_codes), self.fund_codes)
            for handler in logger.handlers:
                handler.flush()
            
        except Exception as e:
            logger.error("解析报告文件失败: %s", e)
            raise

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_fixed(2),
        retry=tenacity.retry_if_exception_type((TimeoutException, WebDriverException)),
        before_sleep=lambda retry_state: logger.info(f"重试基金 {retry_state.args[1]}，第 {retry_state.attempt_number} 次")
    )
    def _get_fund_data_from_dayfund(self, fund_code):
        """使用 Selenium 从 dayfund.cn 抓取基金历史净值数据"""
        logger.info("正在获取基金 %s 的净值数据...", fund_code)
        
        driver = None
        try:
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-infobars')
            options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36')
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            
            url = f"https://www.dayfund.cn/fundvalue/{fund_code}.html"
            driver.set_page_load_timeout(30)  # 页面加载超时30秒
            driver.get(url)
            logger.info("访问URL: %s", url)

            # 检查页面加载状态
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            wait = WebDriverWait(driver, 10)  # 缩短超时到10秒
            table_element = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
            logger.info("表格元素加载完成")
            
            # 优先尝试 lxml 解析器，失败则用 html5lib
            try:
                df_list = pd.read_html(StringIO(driver.page_source), flavor='lxml')
            except ValueError:
                logger.info("lxml 解析失败，尝试 html5lib")
                df_list = pd.read_html(StringIO(driver.page_source), flavor='html5lib')
            
            if not df_list:
                raise ValueError("未找到任何表格")
            
            df = None
            for temp_df in df_list:
                if len(temp_df.columns) >= 4 and ('净值日期' in str(temp_df.columns) or '日期' in str(temp_df.iloc[0, 0]) if not temp_df.empty else False):
                    df = temp_df
                    break
            
            if df is None:
                raise ValueError("未找到有效的净值表格")
            
            df = df.iloc[:, [0, 3]]  # 提取日期和净值列
            df.columns = ['date', 'net_value']
            
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['net_value'] = pd.to_numeric(df['net_value'], errors='coerce')
            df = df.dropna(subset=['date', 'net_value'])
            df = df.drop_duplicates(subset=['date']).sort_values(by='date', ascending=True)
            
            if df.empty:
                raise ValueError("清洗后数据为空")
            
            df = df.tail(100)  # 限制最近100天数据
            logger.info("成功解析基金 %s 的数据，行数: %d, 最新日期: %s, 最新净值: %.4f", 
                        fund_code, len(df), df['date'].iloc[-1], df['net_value'].iloc[-1])
            return df[['date', 'net_value']]

        except Exception as e:
            logger.error("Selenium 抓取基金 %s 失败: %s", fund_code, str(e))
            if driver:
                try:
                    with open(f"error_page_{fund_code}.html", "w", encoding="utf-8") as f:
                        f.write(driver.page_source[:2000])
                    logger.info("错误页面已保存到 error_page_%s.html", fund_code)
                except:
                    pass
            raise  # 抛出异常以触发重试
        finally:
            if driver:
                driver.quit()

    def get_fund_data(self):
        """获取所有基金的数据"""
        logger.info("开始获取 %d 个基金的数据...", len(self.fund_codes))
        for i, fund_code in enumerate(self.fund_codes, 1):
            try:
                logger.info("处理第 %d/%d 个基金: %s", i, len(self.fund_codes), fund_code)
                df = self._get_fund_data_from_dayfund(fund_code)
                
                if df is not None and not df.empty and len(df) >= 14:
                    df = df.sort_values(by='date', ascending=True)
                    delta = df['net_value'].diff()
                    gain = delta.where(delta > 0, 0)
                    loss = -delta.where(delta < 0, 0)
                    avg_gain = gain.rolling(window=14, min_periods=1).mean()
                    avg_loss = loss.rolling(window=14, min_periods=1).mean()
                    rs = avg_gain / avg_loss
                    rsi = 100 - (100 / (1 + rs))
                    
                    ma50 = df['net_value'].rolling(window=min(50, len(df)), min_periods=1).mean()
                    
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
                
                for handler in logger.handlers:
                    handler.flush()
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                logger.error("处理基金 %s 时发生异常: %s", fund_code, str(e))
                self.fund_data[fund_code] = None
                for handler in logger.handlers:
                    handler.flush()

    def generate_report(self):
        """生成市场情绪与技术指标监控报告"""
        logger.info("正在生成市场监控报告...")
        with open(self.output_file, 'w', encoding='utf-8') as f:
            f.write(f"# 市场情绪与技术指标监控报告\n\n")
            f.write(f"生成日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"## 推荐基金技术指标 (处理基金数: {len(self.fund_codes)})\n")
            f.write("| 基金代码 | 最新净值 | RSI | 净值/MA50 | 投资建议 |\n")
            f.write("|----------|----------|-----|-----------|----------|\n")
            
            if not self.fund_codes:
                f.write("| 无 | 无数据 | - | - | 请检查 analysis_report.md 是否包含有效基金代码 |\n")
            else:
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
        with open(self.output_file, 'r', encoding='utf-8') as f:
            logger.info("market_monitor_report.md 内容: %s", f.read())
        for handler in logger.handlers:
            handler.flush()

if __name__ == "__main__":
    try:
        logger.info("脚本启动")
        monitor = MarketMonitor()
        monitor._parse_report()
        monitor.get_fund_data()
        monitor.generate_report()
        logger.info("脚本执行完成")
    except Exception as e:
        logger.error("脚本运行失败: %s", e)
        for handler in logger.handlers:
            handler.flush()
        raise
