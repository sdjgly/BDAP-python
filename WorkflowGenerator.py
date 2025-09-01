from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import json
from consul_utils import register_service, deregister_service
from config import SERVICE_NAME
from WorkflowValidator import SimplifiedWorkflowValidator
from call_llm import call_dify

app = FastAPI()

# ����ģ��
class WorkflowGenerationRequest(BaseModel):
    model: str                          # ģ������
    requestId: str                      # ����ID
    user_id: Optional[str] = None       # �û�ID
    conversation_id: Optional[str] = None # �Ի�ID
    user_prompt: str                    # �û���������
    workflow_name: str                  # ����������
    workflow_description: Optional[str] = None  # ����������
    template_type: Optional[str] = "data_processing" # ģ������
    service_type: Optional[str] = "ml"  # ��������
    isWorkFlow: bool = True             # ������������Ϊ true

# ��������Ϣģ��
class WorkflowInfo(BaseModel):
    userId: str                         # �û�ID

# ������ģ��
class SimpleAttribute(BaseModel):
    name: str                           # ������
    value: str                          # ����ֵ
    valueType: str                      # �������ͣ�Int/Double/String/Boolean��

# ��������ģ��
class ComplicatedAttribute(BaseModel):
    name: str                           # ������
    value: Dict[str, Any]              # JSON����

# Դê��ģ��
class SourceAnchor(BaseModel):
    id: str                             # Դ�ڵ����ID

# Ŀ��ê��ģ��
class TargetAnchor(BaseModel):
    id: str                             # Ŀ��ڵ����ID

# ����ê��ģ��
class InputAnchor(BaseModel):
    sourceAnchors: List[SourceAnchor] = []  # Դê���б�

# ���ê��ģ��
class OutputAnchor(BaseModel):
    targetAnchors: List[TargetAnchor] = []  # Ŀ��ê���б�

# �ڵ�ģ��
class Node(BaseModel):
    id: str                             # ���ID
    name: str                           # �������
    seqId: str                          # ���˳��ID
    position: List[int]                 # �ڵ�λ�� [x, y]
    simpleAttributes: List[SimpleAttribute] = []
    complicatedAttributes: List[ComplicatedAttribute] = []
    inputAnchors: List[InputAnchor] = []
    outputAnchors: List[OutputAnchor] = []

# ��Ӧģ��
class WorkflowGenerationResponse(BaseModel):
    requestId: str                      
    conversation_id: Optional[str] = None
    workflow_info: WorkflowInfo         
    nodes: List[Node]                  

@app.on_event("startup")
async def startup_event():
    """��������ʱע�ᵽConsul"""
    SERVICE_PORT = 8004  # �¶˿�
    service_id = register_service(SERVICE_PORT)
    if service_id:
        app.state.service_id = service_id
        print(f"workflow_generator������ע�ᵽConsul������ID: {service_id}")

@app.on_event("shutdown")
async def shutdown_event():
    """����ر�ʱ��Consulע��"""
    if hasattr(app.state, 'service_id'):
        deregister_service(app.state.service_id)

@app.post("/workflow/generate", response_model=WorkflowGenerationResponse)
async def generate_workflow(request: WorkflowGenerationRequest):
    """���ɹ���������Ҫ�ӿ�"""
    try:
        # 1. ֱ�ӵ��ô�ģ�ͣ�dify���Ѿ����ú���ģ�壩
        llm_response, new_conversation_id = await call_dify(
            model=request.model,
            prompt=request.user_prompt,  # ֱ��ʹ���û�����
            user_id=request.user_id or "anonymous",
            conversation_id=request.conversation_id,
            isWorkFlow=request.isWorkFlow  # ���� isWorkFlow ����
        )
        
        # 2. ����LLM��Ӧ
        workflow_structure = parse_llm_response(
            llm_response=llm_response, 
            workflow_name=request.workflow_name, 
            workflow_description=request.workflow_description,
            user_id=request.user_id,
            service_type=request.service_type,
            request_id=request.requestId
        )
        
        # 3. ʹ�ü򻯵Ĺ�����У����
        validator = SimplifiedWorkflowValidator()
        sanitized_workflow, warnings, errors = validator.sanitize(workflow_structure)
        
        if sanitized_workflow is None:
            raise HTTPException(
                status_code=422, 
                detail=f"�������ṹУ��ʧ��: {', '.join(errors)}"
            )
        
        if warnings:
            print(f"������У�龯��: {', '.join(warnings)}")
        
        # 4. ���ع������ṹ
        return WorkflowGenerationResponse(
            requestId=request.requestId,
            conversation_id=new_conversation_id,
            workflow_info=sanitized_workflow["workflow_info"],
            nodes=sanitized_workflow["nodes"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"����������ʧ��: {str(e)}")

def parse_llm_response(llm_response: str, workflow_name: str, workflow_description: str, 
                      user_id: str, service_type: str, request_id: str) -> Dict[str, Any]:
    """����LLM���ص��ı�����ȡJSON�ṹ"""
    try:
        # ����Ԫ���ʽ����Ӧ
        if isinstance(llm_response, tuple):
            llm_response = llm_response[0]
        
        # ����JSON����
        start_idx = llm_response.find('{')
        end_idx = llm_response.rfind('}') + 1
        
        if start_idx != -1 and end_idx != -1:
            json_str = llm_response[start_idx:end_idx]
            workflow_data = json.loads(json_str)
            
            # ȷ�����������ֶ�
            if "workflow_info" not in workflow_data:
                workflow_data["workflow_info"] = {}
            
            if "nodes" not in workflow_data:
                workflow_data["nodes"] = []
            
            # ���û�����Ϣ
            workflow_data["requestId"] = request_id
            workflow_data["conversation_id"] = None  # ���ڵ��ô�����
            
            # ���ù�������Ϣ
            workflow_data["workflow_info"]["userId"] = user_id or "anonymous"
            
            # ����ڵ����ݣ�ȷ�������µĽṹ��ʽ
            for i, node in enumerate(workflow_data["nodes"]):
                # ȷ�������ֶδ���
                if "seqId" not in node:
                    node["seqId"] = node.get("id", f"node_{i}")
                
                if "position" not in node:
                    node["position"] = [100 + i * 200, 100]
                
                # ȷ��λ�ø�ʽ��ȷ
                if isinstance(node["position"], dict):
                    if "x" in node["position"] and "y" in node["position"]:
                        node["position"] = [node["position"]["x"], node["position"]["y"]]
                
                # ��ʼ�������б�
                node.setdefault("simpleAttributes", [])
                node.setdefault("complicatedAttributes", [])
                node.setdefault("inputAnchors", [])
                node.setdefault("outputAnchors", [])
                
                # ����ê��ṹ��ȷ�������¸�ʽ
                for input_anchor in node["inputAnchors"]:
                    input_anchor.setdefault("sourceAnchors", [])
                
                for output_anchor in node["outputAnchors"]:
                    output_anchor.setdefault("targetAnchors", [])
            
            return workflow_data
        else:
            raise ValueError("LLM��Ӧ��δ�ҵ���Ч��JSON�ṹ")
            
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON����ʧ��: {e}")
    except Exception as e:
        raise ValueError(f"����LLM��Ӧʧ��: {e}")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "workflow_generator"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)