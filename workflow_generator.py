from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import json
from consul_utils import register_service, deregister_service
from config import SERVICE_NAME
from WorkflowValidator import SimplifiedWorkflowValidator
from call_llm import call_dify

app = FastAPI()

# 请求模型
class WorkflowGenerationRequest(BaseModel):
    model: str                          # 模型名称
    requestId: str                      # 请求ID
    user_id: Optional[str] = None       # 用户ID
    conversation_id: Optional[str] = None # 对话ID
    user_prompt: str                    # 用户需求描述
    workflow_name: str                  # 工作流名称
    workflow_description: Optional[str] = None  # 工作流描述
    template_type: Optional[str] = "data_processing" # 模板类型
    service_type: Optional[str] = "ml"  # 服务类型
    isWorkFlow: bool = True             # 新增参数，设为 true

# 工作流信息模型
class WorkflowInfo(BaseModel):
    userId: str                         # 用户ID

# 简单属性模型
class SimpleAttribute(BaseModel):
    name: str                           # 属性名
    value: str                          # 属性值
    valueType: str                      # 属性类型（Int/Double/String/Boolean）

# 复杂属性模型
class ComplicatedAttribute(BaseModel):
    name: str                           # 属性名
    value: Dict[str, Any]              # JSON对象

# 源锚点模型
class SourceAnchor(BaseModel):
    id: str                             # 源节点组件ID

# 目标锚点模型
class TargetAnchor(BaseModel):
    id: str                             # 目标节点组件ID

# 输入锚点模型
class InputAnchor(BaseModel):
    sourceAnchors: List[SourceAnchor] = []  # 源锚点列表

# 输出锚点模型
class OutputAnchor(BaseModel):
    targetAnchors: List[TargetAnchor] = []  # 目标锚点列表

# 节点模型
class Node(BaseModel):
    id: str                             # 组件ID
    name: str                           # 组件名称
    seqId: str                          # 组件顺序ID
    position: List[int]                 # 节点位置 [x, y]
    simpleAttributes: List[SimpleAttribute] = []
    complicatedAttributes: List[ComplicatedAttribute] = []
    inputAnchors: List[InputAnchor] = []
    outputAnchors: List[OutputAnchor] = []

# 响应模型
class WorkflowGenerationResponse(BaseModel):
    requestId: str                      
    conversation_id: Optional[str] = None
    workflow_info: WorkflowInfo         
    nodes: List[Node]                  

@app.on_event("startup")
async def startup_event():
    """服务启动时注册到Consul"""
    SERVICE_PORT = 8004  # 新端口
    service_id = register_service(SERVICE_PORT)
    if service_id:
        app.state.service_id = service_id
        print(f"workflow_generator服务已注册到Consul，服务ID: {service_id}")

@app.on_event("shutdown")
async def shutdown_event():
    """服务关闭时从Consul注销"""
    if hasattr(app.state, 'service_id'):
        deregister_service(app.state.service_id)

@app.post("/workflow/generate", response_model=WorkflowGenerationResponse)
async def generate_workflow(request: WorkflowGenerationRequest):
    """生成工作流的主要接口"""
    try:
        # 1. 直接调用大模型（dify上已经配置好了模板）
        llm_response, new_conversation_id = await call_dify(
            model=request.model,
            prompt=request.user_prompt,
            user_id=request.user_id or "anonymous",
            conversation_id=request.conversation_id,
            isWorkFlow=request.isWorkFlow
        )
        
        # 2. 解析LLM响应
        workflow_structure = parse_llm_response(
            llm_response=llm_response, 
            workflow_name=request.workflow_name, 
            workflow_description=request.workflow_description,
            user_id=request.user_id,
            service_type=request.service_type,
            request_id=request.requestId
        )
        
        # 3. 使用简化的工作流校验器
        validator = SimplifiedWorkflowValidator()
        sanitized_workflow, warnings, errors = validator.sanitize(workflow_structure)
        
        if sanitized_workflow is None:
            raise HTTPException(
                status_code=422, 
                detail=f"工作流结构校验失败: {', '.join(errors)}"
            )
        
        if warnings:
            print(f"工作流校验警告: {', '.join(warnings)}")
        
        # 4. 返回工作流结构
        return WorkflowGenerationResponse(
            requestId=request.requestId,
            conversation_id=new_conversation_id,
            workflow_info=sanitized_workflow["workflow_info"],
            nodes=sanitized_workflow["nodes"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"工作流生成失败: {str(e)}")

def parse_llm_response(llm_response: str, workflow_name: str, workflow_description: str, 
                      user_id: str, service_type: str, request_id: str) -> Dict[str, Any]:
    """解析LLM返回的文本，提取JSON结构"""
    try:
        # 处理元组格式的响应
        if isinstance(llm_response, tuple):
            llm_response = llm_response[0]
        
        # 查找JSON内容
        start_idx = llm_response.find('{')
        end_idx = llm_response.rfind('}') + 1
        
        if start_idx != -1 and end_idx != -1:
            json_str = llm_response[start_idx:end_idx]
            workflow_data = json.loads(json_str)
            
            # 确保包含必需字段
            if "workflow_info" not in workflow_data:
                workflow_data["workflow_info"] = {}
            
            if "nodes" not in workflow_data:
                workflow_data["nodes"] = []
            
            # 设置基本信息
            workflow_data["requestId"] = request_id
            workflow_data["conversation_id"] = None  # 将在调用处设置
            
            # 设置工作流信息
            workflow_data["workflow_info"]["userId"] = user_id or "anonymous"
            
            # 处理节点数据，确保符合新的结构格式
            for i, node in enumerate(workflow_data["nodes"]):
                # 确保必需字段存在
                if "seqId" not in node:
                    node["seqId"] = node.get("id", f"node_{i}")
                
                if "position" not in node:
                    node["position"] = [100 + i * 200, 100]
                
                # 确保位置格式正确
                if isinstance(node["position"], dict):
                    if "x" in node["position"] and "y" in node["position"]:
                        node["position"] = [node["position"]["x"], node["position"]["y"]]
                
                # 初始化属性列表
                node.setdefault("simpleAttributes", [])
                node.setdefault("complicatedAttributes", [])
                node.setdefault("inputAnchors", [])
                node.setdefault("outputAnchors", [])
                
                # 处理锚点结构，确保符合新格式
                for input_anchor in node["inputAnchors"]:
                    input_anchor.setdefault("sourceAnchors", [])
                
                for output_anchor in node["outputAnchors"]:
                    output_anchor.setdefault("targetAnchors", [])
            
            return workflow_data
        else:
            raise ValueError("LLM响应中未找到有效的JSON结构")
            
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON解析失败: {e}")
    except Exception as e:
        raise ValueError(f"解析LLM响应失败: {e}")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "workflow_generator"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
