import time
import traceback
import threading
from datetime import datetime
from functools import wraps
import pytz
import signal
from typing import List, Dict, Any, Optional, Callable
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn
import requests
from abc import ABC, abstractmethod
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore

# 配置
class Config:
    def __init__(self):
        self.title = "Hot News Browser"
        self.description = "A simple hot news browser for Douyin"
        self.version = "1.0.0"
        self.host = "0.0.0.0"
        self.port = 5001
        self.debug = True
        self.cors = {
            "allow_origins": ["*"],
            "allow_credentials": True,
            "allow_methods": ["*"],
            "allow_headers": ["*"],
        }
        self.crawler_interval = 300
        self.crawler_timeout = 60
        self.crawler_max_retry_count = 3
        self.crawler_max_instances = 1
        self.crawler_misfire_grace_time = 60
        self.scheduler_thread_pool_size = 10
        self.scheduler_process_pool_size = 3
        self.scheduler_coalesce = True
        self.scheduler_max_instances = 1
        self.scheduler_misfire_grace_time = 60
        self.scheduler_timezone = "Asia/Shanghai"

# 全局配置
config = Config()

# 简单的内存缓存实现
memory_cache = {}
DEFAULT_EXPIRE = 3600

# 日志实现
class Logger:
    def info(self, message):
        print(f"INFO: {message}")
    def error(self, message):
        print(f"ERROR: {message}")
    def warning(self, message):
        print(f"WARNING: {message}")

log = Logger()

# 缓存函数
def init_cache():
    log.info("Cache initialized")

def close_cache():
    log.info("Cache connection closed")

def set_cache(key: str, value: Any, expire: int = DEFAULT_EXPIRE) -> bool:
    try:
        if isinstance(value, (dict, list, tuple)):
            value = json.dumps(value)
        elif isinstance(value, bool):
            value = "1" if value else "0"

        memory_cache[key] = {
            "value": value,
            "expire": time.time() + expire if expire > 0 else None
        }
        return True
    except Exception as e:
        log.error(f"Error setting cache for key {key}: {e}")
        return False

def get_cache(key: str) -> Optional[Any]:
    try:
        if key not in memory_cache:
            return None

        item = memory_cache[key]
        if item["expire"] and time.time() > item["expire"]:
            del memory_cache[key]
            return None

        value = item["value"]
        try:
            return json.loads(value)
        except:
            return value
    except Exception as e:
        log.error(f"Error getting cache for key {key}: {e}")
        return None

def delete_cache(key: str) -> bool:
    try:
        if key in memory_cache:
            del memory_cache[key]
        return True
    except Exception as e:
        log.error(f"Error deleting cache for key {key}: {e}")
        return False

def get(key):
    return get_cache(key)

def set(key, value, ex=None):
    return set_cache(key, value, ex or DEFAULT_EXPIRE)

def hset(name, key, value):
    try:
        hash_key = f"{name}:{key}"
        return set_cache(hash_key, value)
    except Exception as e:
        log.error(f"Error setting hash cache: {e}")
        return None

def hget(name, key):
    try:
        hash_key = f"{name}:{key}"
        return get_cache(hash_key)
    except Exception as e:
        log.error(f"Error getting hash cache: {e}")
        return None

# 数据库函数
def init_db():
    log.info("Database initialized")

def close_db():
    log.info("Database connection closed")

# 爬虫抽象类
class Crawler(ABC):
    def __init__(self):
        self.header = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
                      "*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/86.0.4240.183 Safari/537.36"
        }
        self.timeout = 10
    
    @abstractmethod
    def fetch(self, date_str: str) -> List[Dict[str, Any]]:
        """获取新闻列表"""
        pass
    
    @abstractmethod
    def crawler_name(self) -> str:
        """获取爬虫名称"""
        pass

# 抖音爬虫
class DouYinCrawler(Crawler):
    def fetch(self, date_str):
        return self.fetch_v2(date_str)

    def fetch_v1(self, date_str):
        # 由于移除了浏览器管理器，这个方法暂时不可用
        return []

    def fetch_v2(self, date_str):
        current_time = datetime.now(pytz.timezone('Asia/Shanghai'))
        url = "https://www.douyin.com/aweme/v1/web/hot/search/list/?device_platform=webapp&aid=6383&channel=channel_pc_web&detail_list=1&source=6&pc_client_type=1&pc_libra_divert=Windows&support_h265=1&support_dash=1&version_code=170400&version_name=17.4.0&cookie_enabled=true&screen_width=1920&screen_height=1080&browser_language=zh-CN&browser_platform=Win32&browser_name=Chrome&browser_version=136.0.0.0&browser_online=true&engine_name=Blink&engine_version=136.0.0.0&os_name=Windows&os_version=10&cpu_core_num=16&device_memory=8&platform=PC&downlink=10&effective_type=4g&round_trip_time=50&webid=7490997798633555467"

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "Chrome/122.0.0.0 Safari/537.36"
                "AppleWebKit/537.36 (KHTML, like Gecko) "
            ),
            "Referer": "https://www.douyin.com/",
        }

        resp = requests.get(url=url, headers=headers, verify=False, timeout=self.timeout)
        if resp.status_code != 200:
            print(f"request failed, status: {resp.status_code}")
            return []

        data = resp.json()
        result = []
        cache_list = []

        for item in data["data"]["word_list"]:
            title = item["word"]
            url =  f"https://www.douyin.com/hot/{item['sentence_id']}?&trending_topic={item['word']}&previous_page=main_page&enter_method=trending_topic&modeFrom=hotDetail&tab_name=trend&position=1&hotValue={item['hot_value']}"

            news = {
                'title': title,
                'url': url,
                'content': title,
                'source': 'douyin',
                'publish_time': current_time.strftime('%Y-%m-%d %H:%M:%S')
            }

            result.append(news)
            cache_list.append(news)

        hset(date_str, self.crawler_name(), json.dumps(cache_list, ensure_ascii=False))
        return result

    def crawler_name(self):
        return "douyin"

# 爬虫工厂
class CrawlerRegister:
    def register(self):
        """注册所有爬虫"""
        crawlers = {}
        
        # 只注册抖音爬虫
        douyin_crawler = DouYinCrawler()
        crawlers["douyin"] = douyin_crawler
        
        return crawlers

# 调度器设置
jobstores = {
    'default': MemoryJobStore()
}

executors = {
    'default': ThreadPoolExecutor(config.scheduler_thread_pool_size),
    'processpool': ProcessPoolExecutor(config.scheduler_process_pool_size)
}

job_defaults = {
    'coalesce': config.scheduler_coalesce,
    'max_instances': config.scheduler_max_instances,
    'misfire_grace_time': config.scheduler_misfire_grace_time,
}

# 创建并配置调度器
_scheduler = BackgroundScheduler(
    jobstores=jobstores,
    executors=executors,
    job_defaults=job_defaults,
    timezone=pytz.timezone(config.scheduler_timezone)
)

# 启动调度器
_scheduler.start()
log.info(f"Scheduler started with timezone: {config.scheduler_timezone}")

# 创建爬虫工厂
crawler_factory = CrawlerRegister().register()

# 爬虫相关常量
CRAWLER_INTERVAL = config.crawler_interval
CRAWLER_TIMEOUT = config.crawler_timeout
MAX_RETRY_COUNT = config.crawler_max_retry_count
SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')

# 爬虫异常
class CrawlerTimeoutError(Exception):
    """爬虫超时异常"""
    pass

# 超时处理装饰器
def timeout_handler(func: Callable, timeout: int = CRAWLER_TIMEOUT) -> Callable:
    """超时处理装饰器，支持Unix信号和线程两种实现"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # 线程实现的超时机制
        result = [None]
        exception = [None]
        completed = [False]
        
        def target():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as e:
                exception[0] = e
            finally:
                completed[0] = True
        
        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(timeout)
        
        if not completed[0]:
            error_msg = f"Function {func.__name__} timed out after {timeout} seconds"
            log.error(error_msg)
            raise CrawlerTimeoutError(error_msg)
        
        if exception[0]:
            log.error(f"Function {func.__name__} raised an exception: {exception[0]}")
            raise exception[0]
                
        return result[0]
    return wrapper

# 安全抓取函数
def safe_fetch(crawler_name: str, crawler, date_str: str, is_retry: bool = False) -> List[Dict[str, Any]]:
    """安全地执行爬虫抓取，处理异常并返回结果"""
    try:
        news_list = crawler.fetch(date_str)
        if news_list and len(news_list) > 0:
            cache_key = f"crawler:{crawler_name}:{date_str}"
            set_cache(key=cache_key, value=news_list, expire=0)
            
            log.info(f"{crawler_name} fetch success, {len(news_list)} news fetched")
            return news_list
        else:
            log.info(f"{'Second time ' if is_retry else ''}crawler {crawler_name} failed. 0 news fetched")
            return []
    except Exception as e:
        error_msg = traceback.format_exc()
        log.error(f"{'Second time ' if is_retry else ''}crawler {crawler_name} error: {error_msg}")
        return []

# 爬虫主逻辑
def crawlers_logic():
    """爬虫主逻辑，包含超时保护和错误处理"""
    
    @timeout_handler
    def crawler_work():
        now_time = datetime.now(SHANGHAI_TZ)
        date_str = now_time.strftime("%Y-%m-%d")
        log.info(f"Starting crawler job at {now_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        retry_crawler = []
        success_count = 0
        failed_crawlers = []
        
        for crawler_name, crawler in crawler_factory.items():
            news_list = safe_fetch(crawler_name, crawler, date_str)
            if news_list:
                success_count += 1
            else:
                retry_crawler.append(crawler_name)
                failed_crawlers.append(crawler_name)
        
        # 第二轮爬取（重试失败的爬虫）
        if retry_crawler:
            log.info(f"Retrying {len(retry_crawler)} failed crawlers")
            retry_failed = []
            for crawler_name in retry_crawler:
                news_list = safe_fetch(crawler_name, crawler_factory[crawler_name], date_str, is_retry=True)
                if news_list:
                    success_count += 1
                    # 从失败列表中移除成功的爬虫
                    if crawler_name in failed_crawlers:
                        failed_crawlers.remove(crawler_name)
                else:
                    retry_failed.append(crawler_name)
        
        # 记录完成时间
        end_time = datetime.now(SHANGHAI_TZ)
        duration = (end_time - now_time).total_seconds()
        log.info(f"Crawler job finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}, "
                 f"duration: {duration:.2f}s, success: {success_count}/{len(crawler_factory)}")
        
        # 爬取完成后执行数据分析
        log.info("Crawler job completed, starting data analysis...")
        
        return success_count
    
    try:
        return crawler_work()
    except CrawlerTimeoutError as e:
        log.error(f"Crawler job timeout: {str(e)}")
        return 0
    except Exception as e:
        log.error(f"Crawler job error: {str(e)}")
        log.error(traceback.format_exc())
        return 0

# 添加爬虫调度任务
_scheduler.add_job(
    crawlers_logic,
    'interval',
    id='crawlers_logic',
    seconds=config.crawler_interval,
    max_instances=config.crawler_max_instances,
    misfire_grace_time=config.crawler_misfire_grace_time
)

# 应用启动和关闭的生命周期管理
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行
    log.info("Application startup")
    
    # 初始化数据库连接
    init_db()
    
    # 初始化缓存
    init_cache()
    
    # 异步启动爬虫，避免阻塞应用启动
    threading.Thread(target=crawlers_logic, daemon=True).start()
    
    yield
    
    # 关闭时执行
    log.info("Application shutdown")
    
    # 关闭数据库连接
    close_db()
    
    # 关闭缓存连接
    close_cache()

# 创建应用实例
app = FastAPI(
    title=config.title,
    description=config.description,
    version=config.version,
    lifespan=lifespan
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.cors["allow_origins"],
    allow_credentials=config.cors["allow_credentials"],
    allow_methods=config.cors["allow_methods"],
    allow_headers=config.cors["allow_headers"],
)

# 请求计时中间件
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# 健康检查端点
@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "healthy", "version": config.version}

# 注册路由
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def get_hot_news(date: str = None, platform: str = None):
    # 平台到爬虫类的映射
    crawler_classes = {
        "douyin": DouYinCrawler,
    }
    
    if platform not in crawler_classes:
        return {
            "status": "404",
            "data": [],
            "msg": "`platform` is required, valid platform: douyin"
        }

    if not date:
        date = datetime.now(pytz.timezone('Asia/Shanghai')).strftime("%Y-%m-%d")

    cacheKey = f"crawler:{platform}:{date}"
    
    # 尝试从缓存获取数据
    try:
        result = get(cacheKey)
        if result:
            try:
                return {
                    "status": "200",
                    "data": result,
                    "msg": "success"
                }
            except Exception as e:
                log.error(f"Error parsing cached data: {e}")
    except Exception as e:
        log.error(f"Error getting cache: {e}")

    # 根据平台获取爬虫实例并获取数据
    try:
        # 平台到爬虫类的映射
        crawler_classes = {
            "douyin": DouYinCrawler,
        }
        
        # 获取对应的爬虫类
        crawler_class = crawler_classes.get(platform)
        if not crawler_class:
            log.error(f"No crawler found for platform: {platform}")
            return {
                "status": "404",
                "data": [],
                "msg": f"No crawler found for platform: {platform}"
            }
        
        # 创建爬虫实例并获取数据
        crawler = crawler_class()
        log.info(f"Created crawler for platform {platform}: {crawler}")
        
        news_list = crawler.fetch(date)
        log.info(f"Crawler returned {len(news_list)} news items for platform {platform}")
        
        if news_list:
            log.info(f"First news item: {news_list[0] if len(news_list) > 0 else 'None'}")
            # 尝试缓存数据
            try:
                set(cacheKey, json.dumps(news_list, ensure_ascii=False))
            except Exception as e:
                log.error(f"Error setting cache: {e}")
            return {
                "status": "200",
                "data": news_list,
                "msg": "success"
            }
        else:
            log.warning(f"Crawler returned empty list for platform {platform}")
            return {
                "status": "200",
                "data": [],
                "msg": "success"
            }
    except Exception as e:
        log.error(f"Error fetching news from crawler: {e}")
        import traceback
        log.error(traceback.format_exc())

    return {
        "status": "200",
        "data": [],
        "msg": "success"
    }

@router.get("/all")
def get_all_platforms_news(date: str = None):
    """
    获取所有平台的热门新闻
    
    Args:
        date: 日期，格式为YYYY-MM-DD，默认为当天
    
    Returns:
        包含所有平台新闻的字典，键为平台名称，值为新闻列表
    """
    # 平台到爬虫类的映射
    crawler_classes = {
        "douyin": DouYinCrawler,
    }
    
    if not date:
        date = datetime.now(pytz.timezone('Asia/Shanghai')).strftime("%Y-%m-%d")
    
    all_news = {}
    
    for platform in crawler_classes.keys():
        cacheKey = f"crawler:{platform}:{date}"
        result = get(cacheKey)
        if result:
            try:
                all_news[platform] = result
            except Exception as e:
                log.error(f"Error parsing cached data for {platform}: {e}")
                all_news[platform] = []
        else:
            all_news[platform] = []
    
    return {
        "status": "200",
        "data": all_news,
        "msg": "success"
    }

@router.get("/multi")
def get_multi_platforms_news(date: str = None, platforms: str = None):
    """
    获取多个平台的热门新闻
    
    Args:
        date: 日期，格式为YYYY-MM-DD，默认为当天
        platforms: 平台列表，以逗号分隔，例如 "douyin"
    
    Returns:
        包含指定平台新闻的字典，键为平台名称，值为新闻列表
    """
    # 平台到爬虫类的映射
    crawler_classes = {
        "douyin": DouYinCrawler,
    }
    
    if not date:
        date = datetime.now(pytz.timezone('Asia/Shanghai')).strftime("%Y-%m-%d")
    
    if not platforms:
        return {
            "status": "404",
            "data": {},
            "msg": "`platforms` parameter is required, format: comma-separated platform names"
        }
    
    platform_list = [p.strip() for p in platforms.split(",")]
    valid_platforms = crawler_classes.keys()
    
    # 验证平台是否有效
    invalid_platforms = [p for p in platform_list if p not in valid_platforms]
    if invalid_platforms:
        return {
            "status": "404",
            "data": {},
            "msg": f"Invalid platforms: {', '.join(invalid_platforms)}. Valid platforms: douyin"
        }
    
    multi_news = {}
    
    for platform in platform_list:
        cacheKey = f"crawler:{platform}:{date}"
        result = get(cacheKey)
        if result:
            try:
                multi_news[platform] = result
            except Exception as e:
                log.error(f"Error parsing cached data for {platform}: {e}")
                multi_news[platform] = []
        else:
            multi_news[platform] = []
    
    return {
        "status": "200",
        "data": multi_news,
        "msg": "success"
    }

@router.get("/search")
def search_news(keyword: str, date: str = None, platforms: str = None, limit: int = 20):
    """
    搜索新闻
    
    Args:
        keyword: 搜索关键词
        date: 日期，格式为YYYY-MM-DD，默认为当天
        platforms: 平台列表，以逗号分隔，例如 "douyin"，默认搜索所有平台
        limit: 返回结果数量限制，默认为20
    
    Returns:
        包含搜索结果的字典，键为状态码、数据、消息、总结果数量和搜索结果数量
    """
    # 平台到爬虫类的映射
    crawler_classes = {
        "douyin": DouYinCrawler,
    }
    
    if not date:
        date = datetime.now(pytz.timezone('Asia/Shanghai')).strftime("%Y-%m-%d")
    
    # 确定要搜索的平台
    if platforms:
        platform_list = [p.strip() for p in platforms.split(",")]
        valid_platforms = crawler_classes.keys()
        platform_list = [p for p in platform_list if p in valid_platforms]
    else:
        platform_list = list(crawler_classes.keys())
    
    if not platform_list:
        return {
            "status": "404",
            "data": [],
            "msg": "No valid platforms specified",
            "total": 0,
            "search_results": 0
        }
    
    # 从各平台获取新闻数据
    all_news = []
    
    for platform in platform_list:
        cacheKey = f"crawler:{platform}:{date}"
        result = get(cacheKey)
        if not result:
            continue
        
        try:
            platform_news = result
            if not isinstance(platform_news, list):
                continue
            
            # 为每条新闻添加平台信息
            for idx, item in enumerate(platform_news):
                if not isinstance(item, dict):
                    continue
                
                # 处理rank字段
                rank_value = ""
                if "rank" in item and item["rank"]:
                    rank_value = str(item["rank"]).replace("#", "")
                elif "index" in item and item["index"]:
                    rank_value = str(item["index"]).replace("#", "")
                else:
                    rank_value = str(idx + 1)
                
                # 获取分类信息
                category = _get_category_for_platform(platform)
                sub_category = _get_subcategory_for_platform(platform)
                
                # 构建标准化的新闻条目
                item_with_source = {
                    "id": item.get("id"),
                    "title": item.get("title", ""),
                    "source": platform,
                    "rank": rank_value,
                    "category": category,
                    "sub_category": sub_category,
                    "url": item.get("url", "")
                }
                all_news.append(item_with_source)
                
        except Exception as e:
            log.error(f"Error processing news from {platform}: {e}")
    
    # 搜索关键词
    search_results = []
    for item in all_news:
        if keyword.lower() in item["title"].lower():
            search_results.append(item)
    
    # 按站点分组，每个站点内按排名排序
    grouped_results = {}
    for item in search_results:
        source = item["source"]
        if source not in grouped_results:
            grouped_results[source] = []
        grouped_results[source].append(item)
    
    # 对每个站点内的结果按排名排序
    for source, items in grouped_results.items():
        # 按排名排序（直接比较数字）
        items.sort(key=lambda x: int(x["rank"]) if x["rank"].isdigit() else 999)
    
    # 重新组合排序后的结果
    sorted_results = []
    for source, items in grouped_results.items():
        sorted_results.extend(items)
    
    # 限制返回结果数量
    limited_results = sorted_results[:limit]
    
    return {
        "status": "200",
        "data": limited_results,
        "msg": "success",
        "total": len(search_results),
        "search_results": len(limited_results)
    }

def _get_category_for_platform(platform: str) -> str:
    """根据平台返回对应的分类"""
    categories = {
        "douyin": "娱乐",
    }
    return categories.get(platform, "其他")

def _get_subcategory_for_platform(platform: str) -> str:
    """根据平台返回对应的子分类"""
    subcategories = {
        "douyin": "娱乐",
    }
    return subcategories.get(platform, "其他")

# 注册路由
app.include_router(router, prefix="/api/v1/dailynews", tags=["Daily News"])

# 挂载静态文件目录
app.mount("/", StaticFiles(directory=".", html=True), name="static")

# 如果直接运行此文件
if __name__ == "__main__":
    uvicorn.run("single_file_app:app", host=config.host, port=config.port, reload=config.debug)