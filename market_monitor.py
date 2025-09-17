import pandas as pd
import numpy as np
import re
import os
import logging
from datetime import datetime
import time
import random
import requests
import json
import hashlib

# 配置日志
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

    def _get_fund_data_from_eastmoney(self, fund_code):
        """从东方财富网 API 抓取基金历史净值数据"""
        logger.info("正在获取基金 %s 的净值数据...", fund_code)
        
        try:
            # 生成随机 rt 参数避免缓存
            rt = round(random.uniform(0.1, 0.9), 6)
            url = f"https://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={fund_code}&page=1&per=5000&sdate=&edate=&rt={rt}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # 解析 JSON
            data = json.loads(response.text)
            if 'DataLSJZ' not in data or not data['DataLSJZ']:
                raise ValueError("API 返回空数据")
            
            # 解析净值数据
            rows = data['DataLSJZ'].split(';')
            net_values = []
            dates = []
            for row in rows:
                if row.strip():
                    parts = row.strip().split(',')
                    if len(parts) >= 2:
                        date = parts[0].replace('Date:', '').strip("'")
                        net_value = float(parts[1].replace('NetValue:', '').strip())
                        dates.append(date)
                        net_values.append(net_value)
            
            df = pd.DataFrame({'date': dates, 'net_value': net_values})
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date', 'net_value']).sort_values(by='date', ascending=True)
            
            if df.empty:
                raise ValueError("清洗后数据为空")
            
            # 限制最近100天数据
            df = df.tail(100)
            logger.info("成功解析基金 %s 的数据，行数: %d, 最新日期: %s, 最新净值: %.4f", 
                        fund_code, len(df), df['date'].iloc[-1], df['net_value'].iloc[-1])
            return df[['date', 'net_value']]

        except Exception as e:
            logger.error("API 抓取基金 %s 失败: %s", fund_code, str(e))
            return None

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
                time.sleep(random.uniform(0.5, 1.5))  # 减少延迟
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
