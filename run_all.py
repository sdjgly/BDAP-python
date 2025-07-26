import subprocess
import os


# 启动 call_llm（绑定8000端口）
subprocess.Popen(["uvicorn", "call_llm:app", "--host", "0.0.0.0", "--port", "8000"])

# 启动 queue_service（绑定8001端口）
subprocess.Popen(["uvicorn", "queue_service:app", "--host", "0.0.0.0", "--port", "8001"])

print("两个服务已启动:")

print("- call_llm服务: http://0.0.0.0:8000")

print("- queue_service服务: http://0.0.0.0:8001")

print("服务将自动注册到Consul")

# 阻塞等待，让脚本不退出
import time
while True:
    time.sleep(1)
