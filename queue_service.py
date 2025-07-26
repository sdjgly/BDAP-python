import httpx
from fastapi import FastAPI, HTTPException
from consul_utils import register_service, deregister_service
from config import CONSUL_HOST, CONSUL_PORT, SERVICE_NAME
from pydantic import BaseModel
from typing import Optional, Dict, List
import asyncio
import threading
from queue import Queue
import time
from datetime import datetime
import uuid
import consul
import socket
import os
import atexit
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProcessRequest(BaseModel):
    requestId: str
    model: str
    prompt: str

class ProcessResponse(BaseModel):
    status: str
    requestId: str
    model: str
    queuePosition: int

class QueueInfo(BaseModel):
    queueLength: int
    processingCount: int

class CompletedRequest(BaseModel):
    requestId: str
    model: str
    status: str
    result: str
    timestamp: str

class QueueUpdateRequest(BaseModel):
    queues: Dict[str, QueueInfo]
    completedRequests: List[CompletedRequest]

class QueueManager:
    def __init__(self):
        self.queues: Dict[str, Queue] = {}
        self.processing_count: Dict[str, int] = {}
        self.completed_requests: Dict[str, dict] = {}
        self.processing_requests: Dict[str, dict] = {}  # 新增：正在处理的请求
        self.workers: Dict[str, threading.Thread] = {}
        self.running = True
        
        # 实现队列信息更新
        self.pushed_request_ids: set = set()
        self.push_lock = threading.Lock()
        
        # 创建一个新的事件循环用于工作线程
        self.worker_loop = None
        self.loop_thread = None
        
        # 初始化模型队列
        for model in ["OpenAI", "silicon-flow", "moonshot"]:
            self.queues[model] = Queue()
            self.processing_count[model] = 0
        
        # 启动工作线程事件循环
        self.start_worker_loop()
        
        # 启动各模型的工作线程
        for model in ["OpenAI", "silicon-flow", "moonshot"]:
            self.start_worker(model)

    def start_worker_loop(self):
        """启动专门用于工作线程的事件循环"""
        def run_loop():
            self.worker_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.worker_loop)
            self.worker_loop.run_forever()
        
        self.loop_thread = threading.Thread(target=run_loop, daemon=True)
        self.loop_thread.start()
        
        # 等待事件循环启动
        while self.worker_loop is None:
            time.sleep(0.01)

    def start_background_push_loop(self):
        """启动后台推送任务"""
        loop = asyncio.get_event_loop()
        loop.create_task(self.periodic_push_to_java())

    def start_worker(self, model: str):
        """启动工作线程"""
        worker = threading.Thread(target=self._worker, args=(model,), daemon=True)
        worker.start()
        self.workers[model] = worker

    def _worker(self, model: str):
        """工作线程主函数"""
        while self.running:
            try:
                if not self.queues[model].empty():
                    request_data = self.queues[model].get(timeout=1)
                    logger.info(f"Processing request {request_data['requestId']} for model {model}")
                    
                    # 标记为正在处理
                    self.processing_requests[request_data["requestId"]] = {
                        "requestId": request_data["requestId"],
                        "model": model,
                        "status": "processing",
                        "timestamp": datetime.now().isoformat() + "Z"
                    }
                    
                    self.processing_count[model] += 1
                    
                    # 使用工作线程的事件循环执行异步任务
                    future = asyncio.run_coroutine_threadsafe(
                        self._process_request(request_data, model), 
                        self.worker_loop
                    )
                    
                    try:
                        future.result()  # 等待完成
                    except Exception as e:
                        logger.error(f"Error processing request {request_data['requestId']}: {e}")
                        # 处理失败时也要记录结果
                        self.completed_requests[request_data["requestId"]] = {
                            "requestId": request_data["requestId"],
                            "model": model,
                            "status": "failed",
                            "result": f"Worker error: {str(e)}",
                            "timestamp": datetime.now().isoformat() + "Z"
                        }
                    
                    self.processing_count[model] -= 1
                    
                    # 从正在处理列表中移除
                    if request_data["requestId"] in self.processing_requests:
                        del self.processing_requests[request_data["requestId"]]
                    
                    self.queues[model].task_done()
                else:
                    time.sleep(0.1)
            except Exception as e:
                logger.error(f"Worker thread error for model {model}: {e}")
                time.sleep(1)  # 发生错误时等待更长时间

    async def _process_request(self, request_data: dict, model: str):
        """处理单个请求"""
        try:
            # 使用环境变量或配置文件中的端口，默认8000
            call_llm_port = os.getenv("CALL_LLM_PORT", "8000")
            call_llm_url = f"http://localhost:{call_llm_port}/llm"
            
            logger.info(f"Calling LLM service at {call_llm_url} for request {request_data['requestId']}")
            
            timeout = httpx.Timeout(120.0, read=120.0, connect=10.0)
            async with httpx.AsyncClient(timeout=timeout, proxies=None) as client:
                response = await client.post(
                    call_llm_url,
                    json={
                        "model": model,
                        "question": request_data["prompt"],
                        "requestId": request_data["requestId"]
                    }
                )

            logger.info(f"LLM service response status: {response.status_code} for request {request_data['requestId']}")

            if response.status_code == 200:
                result_data = response.json()
                result = result_data.get("answer", "[错误] 未返回answer字段")
                self.completed_requests[request_data["requestId"]] = {
                    "requestId": request_data["requestId"],
                    "model": model,
                    "status": "completed",
                    "result": result,
                    "timestamp": datetime.now().isoformat() + "Z"
                }
                logger.info(f"Request {request_data['requestId']} completed successfully")
            else:
                error_msg = f"[HTTP错误] 状态码: {response.status_code}, 内容: {response.text}"
                self.completed_requests[request_data["requestId"]] = {
                    "requestId": request_data["requestId"],
                    "model": model,
                    "status": "failed",
                    "result": error_msg,
                    "timestamp": datetime.now().isoformat() + "Z"
                }
                logger.error(f"Request {request_data['requestId']} failed: {error_msg}")
                
        except Exception as e:
            error_msg = f"Processing failed: {str(e)}"
            self.completed_requests[request_data["requestId"]] = {
                "requestId": request_data["requestId"],
                "model": model,
                "status": "failed",
                "result": error_msg,
                "timestamp": datetime.now().isoformat() + "Z"
            }
            logger.error(f"Exception processing request {request_data['requestId']}: {e}")

    def add_request(self, request: ProcessRequest) -> int:
        """添加请求到队列"""
        request_data = {
            "requestId": request.requestId,
            "prompt": request.prompt
        }

        if request.model not in self.queues:
            self.queues[request.model] = Queue()
            self.processing_count[request.model] = 0
            self.start_worker(request.model)

        self.queues[request.model].put(request_data)
        queue_position = self.queues[request.model].qsize()
        
        logger.info(f"Request {request.requestId} added to {request.model} queue, position: {queue_position}")
        return queue_position

    def get_queue_position(self, model: str) -> int:
        """获取队列位置"""
        return self.queues.get(model, Queue()).qsize()
    
    def get_request_status(self, request_id: str) -> dict:
        """获取请求状态"""
        # 检查是否已完成
        if request_id in self.completed_requests:
            return self.completed_requests[request_id]
        
        # 检查是否正在处理
        if request_id in self.processing_requests:
            return self.processing_requests[request_id]
        
        # 检查是否在队列中等待
        for model, queue in self.queues.items():
            # 这里需要遍历队列查找，但Queue不支持直接遍历
            # 可以考虑使用额外的数据结构来跟踪队列中的请求
            pass
        
        return None

    async def periodic_push_to_java(self, interval: float = 5.0, batch_size: int = 3):
        """定期推送到Java后端"""
        while self.running:
            try:
                await asyncio.sleep(interval)

                new_completed = []
                with self.push_lock:
                    for req_id, data in self.completed_requests.items():
                        if req_id not in self.pushed_request_ids:
                            new_completed.append(data)
                            if len(new_completed) >= batch_size:
                                break

                if not new_completed:
                    continue

                payload = {
                    "queues": {
                        model: {
                            "queueLength": self.queues[model].qsize(),
                            "processingCount": self.processing_count[model]
                        } for model in self.queues
                    },
                    "completedRequests": new_completed
                }

                # 使用环境变量配置Java后端地址
                java_backend_port = os.getenv("JAVA_BACKEND_PORT", "7003")
                java_backend_url = f"http://localhost:{java_backend_port}/llm/queue/update"

                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.post(java_backend_url, json=payload)
                    if resp.status_code == 200:
                        with self.push_lock:
                            for item in new_completed:
                                self.pushed_request_ids.add(item["requestId"])
                        logger.info(f"Successfully pushed {len(new_completed)} completed requests to Java backend")
                    else:
                        logger.warning(f"Java backend returned {resp.status_code}: {resp.text}")

            except Exception as e:
                logger.error(f"Push to Java backend failed: {e}")

    def shutdown(self):
        """关闭队列管理器"""
        self.running = False
        if self.worker_loop:
            self.worker_loop.call_soon_threadsafe(self.worker_loop.stop)


# 创建队列管理器实例
queue_manager = QueueManager()
app = FastAPI()

@app.on_event("startup")
async def startup_event():
    """服务启动时注册到Consul"""
    SERVICE_PORT = 8001
    service_id = register_service(SERVICE_PORT)

    if service_id:
        app.state.service_id = service_id
        logger.info(f"queue_service服务已注册到Consul，服务ID: {service_id}")
    
    # 启动后台推送任务
    queue_manager.start_background_push_loop()

@app.on_event("shutdown")
async def shutdown_event():
    """服务关闭时从Consul注销"""
    queue_manager.shutdown()
    
    if hasattr(app.state, 'service_id'):
        deregister_service(app.state.service_id)

# 健康检查端点
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": SERVICE_NAME}

@app.post("/llm/process", response_model=ProcessResponse)
async def process_request(request: ProcessRequest):
    """处理请求"""
    if not request.prompt:
        raise HTTPException(status_code=400, detail="prompt cannot be empty")

    queue_position = queue_manager.add_request(request)

    return ProcessResponse(
        status="queued",
        requestId=request.requestId,
        model=request.model,
        queuePosition=queue_position
    )

@app.post("/llm/queue/update")
async def update_queue_status(request: QueueUpdateRequest):
    """更新队列状态"""
    return {"status": "success", "message": "Queue status updated"}

@app.get("/llm/result/{request_id}")
async def get_result(request_id: str):
    """获取请求结果"""
    if request_id in queue_manager.completed_requests:
        return queue_manager.completed_requests[request_id]
    elif request_id in queue_manager.processing_requests:
        return queue_manager.processing_requests[request_id]
    else:
        raise HTTPException(status_code=404, detail="Request result not found")

@app.get("/llm/queue/status")
async def get_queue_status():
    """获取队列状态"""
    queueLength = 0
    processingCount = 0
    for model, queue in queue_manager.queues.items():
        queueLength += queue.qsize()
        processingCount += queue_manager.processing_count[model]
    
    status = {
        "queueLength": queueLength,
        "processingCount": processingCount,
        "completedCount": len(queue_manager.completed_requests)
    }
    return status

@app.get("/llm/debug/{request_id}")
async def debug_request(request_id: str):
    """调试端点：查看请求状态"""
    status = queue_manager.get_request_status(request_id)
    if status:
        return status
    
    # 提供更详细的调试信息
    debug_info = {
        "request_id": request_id,
        "in_completed": request_id in queue_manager.completed_requests,
        "in_processing": request_id in queue_manager.processing_requests,
        "total_completed": len(queue_manager.completed_requests),
        "total_processing": len(queue_manager.processing_requests),
        "queue_sizes": {model: queue.qsize() for model, queue in queue_manager.queues.items()},
        "processing_counts": queue_manager.processing_count
    }
    
    return debug_info

if __name__ == "__main__":
    import uvicorn

    # 注册服务到Consul
    SERVICE_PORT = 8001
    service_id = register_service(SERVICE_PORT)

    # 程序退出时注销服务
    if service_id:
        atexit.register(deregister_service, service_id)

    SERVICE_PORT = 8001
    uvicorn.run(app, host="localhost", port=SERVICE_PORT)