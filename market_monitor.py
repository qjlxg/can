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
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import requests
import tenacity

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('market_monitor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MarketMonitor:
    def __init__(self, report_file='analysis_report.md', output_file='market_monitor_report.md'):
        self.report_file = report_file
        self.output_file = output_file
        self.fund_codes = []
        self.fund_data = {}

    def _validate_fund_code(self, fund_code):
        """验证基金代码是否有效"""
        try:
            url = f"https://fund.10jqka.com.cn/{fund_code}/historynet.html"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
            }
            response = requests.head(url, headers=headers, timeout=5)
            if response.status_code == 200:
                logger.info("基金代码 %s 有效", fund_code)
                return True
            else:
                logger.warning("基金代码 %s 无效，状态码: %d", fund_code, response.status_code)
                return False
        except Exception as e:
            logger.warning("验证基金代码 %s 失败: %s", fund_code, str(e))
            return False

    def _parse_report(self):
        """从 analysis_report.md 提取推荐基金代码并验证"""
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
            
            # 验证基金代码
            valid_codes = []
            for code in self.fund_codes:
                if self._validate_fund_code(code):
                    valid_codes.append(code)
                time.sleep(random.uniform(0.5, 1))  # 避免请求过快
            
            self.fund_codes = valid_codes[:5]  # 限制前5个有效代码
            if not self.fund_codes:
                logger.warning("未提取到任何有效基金代码，请检查 analysis_report.md")
            else:
                logger.info("提取到 %d 个有效基金（测试限制前5个）: %s", len(self.fund_codes), self.fund_codes)
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
    def _get_fund_data_from_10jqka(self, fund_code):
        """使用 Selenium 从 fund.10jqka.com.cn 抓取基金历史净值数据（含翻页）"""
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
            options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36')
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            
            url = f"https://fund.10jqka.com.cn/{fund_code}/historynet.html"
            driver.set_page_load_timeout(15)  # 页面加载超时15秒
            driver.get(url)
            logger.info("访问URL: %s", url)

            WebDriverWait(driver, 8).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            wait = WebDriverWait(driver, 8)
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, 's-list')))
            logger.info("净值列表容器加载完成")

            # 保存首页面用于调试
            with open(f"debug_page_{fund_code}_page1.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source[:2000])
            logger.info("调试页面已保存到 debug_page_%s_page1.html", fund_code)

            # 初始化数据框
            all_data = []
            
            while True:
                # 解析当前页面表格
                try:
                    table_element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 's-list')))
                    df_list = pd.read_html(StringIO(driver.page_source), flavor='lxml')
                    if not df_list:
                        raise ValueError("未找到任何表格")
                    
                    df = None
                    for temp_df in df_list:
                        if len(temp_df.columns) >= 2 and '日期' in str(temp_df.columns):
                            df = temp_df
                            break
                    
                    if df is None:
                        raise ValueError("未找到有效的净值表格")
                    
                    df = df[['日期', '单位净值（元）']].copy()  # 只取日期和单位净值
                    df.columns = ['date', 'net_value']
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    df['net_value'] = pd.to_numeric(df['net_value'], errors='coerce')
                    df = df.dropna(subset=['date', 'net_value'])
                    all_data.append(df)
                    logger.info("解析基金 %s 当前页面，获取 %d 行数据", fund_code, len(df))
                
                except Exception as e:
                    logger.error("解析基金 %s 当前页面失败: %s", fund_code, str(e))
                    break

                # 检查是否有下一页
                try:
                    next_button = driver.find_element(By.XPATH, "//div[@id='m-turn']//a[contains(text(), '下一页')]")
                    if 'disabled' in next_button.get_attribute('class') or not next_button.is_enabled():
                        logger.info("基金 %s 已到达最后一页", fund_code)
                        break
                    next_button.click()
                    time.sleep(random.uniform(1, 2))  # 等待页面加载
                    WebDriverWait(driver, 8).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 's-list'))
                    )
                    # 保存翻页后的页面
                    page_num = len(all_data) + 1
                    with open(f"debug_page_{fund_code}_page{page_num}.html", "w", encoding="utf-8") as f:
                        f.write(driver.page_source[:2000])
                    logger.info("翻页成功，调试页面保存到 debug_page_%s_page%d.html", fund_code, page_num)
                
                except NoSuchElementException:
                    logger.info("基金 %s 无下一页按钮，结束翻页", fund_code)
                    break
                except Exception as e:
                    logger.error("翻页失败: %s", str(e))
                    break

            # 合并所有页面数据
            if all_data:
                df = pd.concat(all_data, ignore_index=True)
                df = df.drop_duplicates(subset=['date']).sort_values(by='date', ascending=True)
                df = df.tail(100)  # 取最近 100 天
                logger.info("成功解析基金 %s 的数据，行数: %d, 最新日期: %s, 最新净值: %.4f", 
                            fund_code, len(df), df['date'].iloc[-1], df['net_value'].iloc[-1])
                return df[['date', 'net_value']]
            else:
                raise ValueError("未获取到任何有效数据")

        except Exception as e:
            logger.error("Selenium 抓取基金 %s 失败: %s", fund_code, str(e))
            if driver:
                try:
                    with open(f"error_page_{fund_code}.html", "w", encoding="utf-8") as f:
                        f.write(driver.page_source[:2000])
                    logger.info("错误页面已保存到 error_page_%s.html", fund_code)
                except:
                    pass
            raise
        finally:
            if driver:
                driver.quit()

    def get_fund_data(self):
        """获取所有基金的数据"""
        logger.info("开始获取 %d 个基金的数据...", len(self.fund_codes))
        for i, fund_code in enumerate(self.fund_codes, 1):
            try:
                logger.info("处理第 %d/%d 个基金: %s", i, len(self.fund_codes), fund_code)
                df = self._get_fund_data_from_10jqka(fund_code)
                
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
