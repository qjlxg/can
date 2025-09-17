import pandas as pd
import numpy as np
import re
import os
import logging
from datetime import datetime, timedelta
import time
import random
from io import StringIO
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
            url = f"http://fund.eastmoney.com/{fund_code}.html"
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
            
            self.fund_codes = valid_codes[:10]  # 限制前10个有效代码
            if not self.fund_codes:
                logger.warning("未提取到任何有效基金代码，请检查 analysis_report.md")
            else:
                logger.info("提取到 %d 个有效基金（测试限制前10个）: %s", len(self.fund_codes), self.fund_codes)
            for handler in logger.handlers:
                handler.flush()
            
        except Exception as e:
            logger.error("解析报告文件失败: %s", e)
            raise

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_fixed(2),
        retry=tenacity.retry_if_exception_type(requests.exceptions.RequestException),
        before_sleep=lambda retry_state: logger.info(f"重试基金 {retry_state.args[0]}，第 {retry_state.attempt_number} 次")
    )
    def _get_fund_data_from_eastmoney(self, fund_code):
        """
        通过天天基金网的 API 接口获取基金历史净值数据。
        更稳定，无需依赖浏览器驱动。
        """
        logger.info("正在通过 API 获取基金 %s 的净值数据...", fund_code)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Referer': f'http://fundf10.eastmoney.com/jjjz_{fund_code}.html'
        }
        
        # 抓取最近一年的数据，足够计算技术指标
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        url = "http://api.fund.eastmoney.com/f10/lsjz"
        params = {
            'fundCode': fund_code,
            'pageIndex': 1,
            'pageSize': 2000, # 一次性获取足够多的数据，避免翻页
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d')
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data['Data'] and 'LSJZList' in data['Data']:
                df = pd.DataFrame(data['Data']['LSJZList'])
                df['FSRQ'] = pd.to_datetime(df['FSRQ'])
                df['DWJZ'] = pd.to_numeric(df['DWJZ'], errors='coerce')
                df = df.rename(columns={'FSRQ': 'date', 'DWJZ': 'net_value'})
                
                # 清洗数据并按日期排序
                df = df.dropna(subset=['date', 'net_value'])
                df = df.sort_values(by='date', ascending=True).drop_duplicates(subset=['date'])

                logger.info("成功通过 API 获取基金 %s 的数据，行数: %d, 最新日期: %s, 最新净值: %.4f", 
                            fund_code, len(df), df['date'].iloc[-1].strftime('%Y-%m-%d'), df['net_value'].iloc[-1])
                return df[['date', 'net_value']]
            else:
                raise ValueError("API 返回数据中没有找到历史净值列表")

        except requests.exceptions.RequestException as e:
            logger.error("API 请求基金 %s 失败: %s", fund_code, str(e))
            raise
        except (ValueError, KeyError) as e:
            logger.error("解析基金 %s 的 API 响应失败: %s", fund_code, str(e))
            raise

    def get_fund_data(self):
        """获取所有基金的数据"""
        logger.info("开始获取 %d 个基金的数据...", len(self.fund_codes))
        for i, fund_code in enumerate(self.fund_codes, 1):
            try:
                logger.info("处理第 %d/%d 个基金: %s", i, len(self.fund_codes), fund_code)
                df = self._get_fund_data_from_eastmoney(fund_code)
                
                if df is not None and not df.empty and len(df) >= 14:
                    df = df.sort_values(by='date', ascending=True)
                    delta = df['net_value'].diff()
                    gain = delta.where(delta > 0, 0)
                    loss = -delta.where(delta < 0, 0)
                    avg_gain = gain.rolling(window=14, min_periods=1).mean()
                    avg_loss = loss.rolling(window=14, min_periods=1).mean()
                    
                    # 避免除以0
                    rs = avg_gain / avg_loss.replace(0, np.nan)
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
