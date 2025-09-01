from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import fastapi
from fastapi import FastAPI
from consul_utils import register_service, deregister_service
from config import SERVICE_NAME
import atexit

app = FastAPI()


# 添加服务启动和关闭事件
@app.on_event("startup")
async def startup_event():
    """服务启动时注册到Consul"""
    service_id = start_tool_functions_service()
    if service_id:
        app.state.service_id = service_id
        print(f"tool_functions服务已注册到Consul，服务ID: {service_id}")


@app.on_event("shutdown")
async def shutdown_event():
    """服务关闭时从Consul注销"""
    if hasattr(app.state, 'service_id'):
        deregister_service(app.state.service_id)


def start_tool_functions_service():
    SERVICE_PORT = 8003
    tags = ['tools', 'data-processing', 'pandas']
    service_id = register_service(SERVICE_PORT, tags)
    return service_id


# 健康检查端点
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "tool-functions"}


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 可指定特定域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SHARED_DIR = "/root/BDAP-python"

@app.get("/files/{filename}", response_class=FileResponse)
def get_file(filename: str):
    """
    从共享目录返回指定文件
    """
    file_path = os.path.join(SHARED_DIR, filename)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="文件不存在")

    return FileResponse(file_path, filename=filename)
