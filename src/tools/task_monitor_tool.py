"""
任务监控工具 - 查询批量任务运行状态
支持从数据源获取任务的运行信息、状态、日志等
"""

import os
import json
from typing import Optional, List, Dict, Any
from langchain.tools import tool
from datetime import datetime

@tool
def task_monitor(task_id: Optional[str] = None, status: Optional[str] = None) -> str:
    """
    查询批量任务的运行状态和信息
    
    Args:
        task_id: 可选，指定任务ID查询单个任务
        status: 可选，按状态筛选任务 (success, failed, running, pending)
    
    Returns:
        任务运行状态信息，包含任务ID、状态、开始时间、结束时间、执行时长、日志路径等
    """
    workspace_path = os.getenv("COZE_WORKSPACE_PATH", "/workspace/projects")
    data_file = os.path.join(workspace_path, "assets/task_data.json")
    
    # 如果数据文件不存在，创建示例数据
    if not os.path.exists(data_file):
        os.makedirs(os.path.dirname(data_file), exist_ok=True)
        sample_data = {
            "tasks": [
                {
                    "task_id": "task_001",
                    "task_name": "用户数据同步",
                    "status": "success",
                    "start_time": "2025-06-17 08:00:00",
                    "end_time": "2025-06-17 08:15:30",
                    "duration": 930,  # 秒
                    "data_count": 1500000,
                    "log_path": "logs/task_001.log",
                    "error_msg": None
                },
                {
                    "task_id": "task_002",
                    "task_name": "订单数据清洗",
                    "status": "failed",
                    "start_time": "2025-06-17 08:30:00",
                    "end_time": "2025-06-17 08:45:20",
                    "duration": 920,
                    "data_count": 850000,
                    "log_path": "logs/task_002.log",
                    "error_msg": "NullPointerException at DataProcessor.clean()"
                },
                {
                    "task_id": "task_003",
                    "task_name": "销售报表生成",
                    "status": "running",
                    "start_time": "2025-06-17 09:00:00",
                    "end_time": None,
                    "duration": None,
                    "data_count": None,
                    "log_path": "logs/task_003.log",
                    "error_msg": None
                },
                {
                    "task_id": "task_004",
                    "task_name": "产品库存校验",
                    "status": "pending",
                    "start_time": None,
                    "end_time": None,
                    "duration": None,
                    "data_count": None,
                    "log_path": "logs/task_004.log",
                    "error_msg": None
                }
            ]
        }
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, indent=2, ensure_ascii=False)
    
    # 读取任务数据
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    tasks = data.get("tasks", [])
    
    # 根据 task_id 查询单个任务
    if task_id:
        for task in tasks:
            if task["task_id"] == task_id:
                return json.dumps({
                    "success": True,
                    "task": task,
                    "message": f"成功获取任务 {task_id} 的信息"
                }, ensure_ascii=False, indent=2)
        return json.dumps({
            "success": False,
            "message": f"未找到任务ID为 {task_id} 的任务"
        }, ensure_ascii=False, indent=2)
    
    # 根据 status 筛选任务
    if status:
        filtered_tasks = [t for t in tasks if t["status"] == status]
        return json.dumps({
            "success": True,
            "tasks": filtered_tasks,
            "count": len(filtered_tasks),
            "message": f"筛选出 {len(filtered_tasks)} 个状态为 {status} 的任务"
        }, ensure_ascii=False, indent=2)
    
    # 返回所有任务
    return json.dumps({
        "success": True,
        "tasks": tasks,
        "count": len(tasks),
        "summary": {
            "total": len(tasks),
            "success": len([t for t in tasks if t["status"] == "success"]),
            "failed": len([t for t in tasks if t["status"] == "failed"]),
            "running": len([t for t in tasks if t["status"] == "running"]),
            "pending": len([t for t in tasks if t["status"] == "pending"])
        },
        "message": f"当前共有 {len(tasks)} 个批量任务"
    }, ensure_ascii=False, indent=2)


@tool
def task_log_reader(task_id: str, lines: int = 100) -> str:
    """
    读取指定任务的执行日志
    
    Args:
        task_id: 任务ID
        lines: 读取的日志行数，默认100行
    
    Returns:
        任务执行日志内容
    """
    workspace_path = os.getenv("COZE_WORKSPACE_PATH", "/workspace/projects")
    
    # 创建示例日志文件
    logs_dir = os.path.join(workspace_path, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    log_file = os.path.join(logs_dir, f"{task_id}.log")
    
    # 如果日志文件不存在，生成示例日志
    if not os.path.exists(log_file):
        sample_logs = {
            "task_001": f"""[2025-06-17 08:00:00] INFO  Task started: task_001 (用户数据同步)
[2025-06-17 08:00:05] INFO  Connecting to database...
[2025-06-17 08:00:10] INFO  Database connected successfully
[2025-06-17 08:00:15] INFO  Starting data extraction...
[2025-06-17 08:05:00] INFO  Extracted 500000 records
[2025-06-17 08:10:00] INFO  Extracted 1000000 records
[2025-06-17 08:12:00] INFO  Data transformation started
[2025-06-17 08:14:00] INFO  Transformation completed
[2025-06-17 08:15:00] INFO  Loading data to target system...
[2025-06-17 08:15:30] INFO  Task completed successfully. Total records: 1500000
""",
            "task_002": f"""[2025-06-17 08:30:00] INFO  Task started: task_002 (订单数据清洗)
[2025-06-17 08:30:05] INFO  Initializing DataProcessor...
[2025-06-17 08:30:10] INFO  Loading order data...
[2025-06-17 08:35:00] INFO  Data loaded: 850000 records
[2025-06-17 08:35:10] INFO  Starting data cleaning...
[2025-06-17 08:40:00] INFO  Cleaning step 1/3 completed
[2025-06-17 08:42:00] ERROR Exception occurred: NullPointerException
[2025-06-17 08:42:00] ERROR   at DataProcessor.clean(DataProcessor.java:245)
[2025-06-17 08:42:00] ERROR   at DataProcessor.run(DataProcessor.java:120)
[2025-06-17 08:42:00] ERROR Task failed due to exception
[2025-06-17 08:45:20] INFO  Task terminated
""",
            "task_003": f"""[2025-06-17 09:00:00] INFO  Task started: task_003 (销售报表生成)
[2025-06-17 09:00:05] INFO  Querying sales data from database...
[2025-06-17 09:05:00] INFO  Processing sales data...
[2025-06-17 09:10:00] INFO  Generating report charts...
[2025-06-17 09:12:00] INFO  Task is still running...
"""
        }
        
        if task_id in sample_logs:
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(sample_logs[task_id])
        else:
            return f"错误：未找到任务 {task_id} 的日志文件"
    
    # 读取日志文件
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            all_lines = f.readlines()
        
        # 获取最后 N 行
        last_lines = all_lines[-lines:] if lines < len(all_lines) else all_lines
        
        return json.dumps({
            "success": True,
            "task_id": task_id,
            "log_lines": len(last_lines),
            "log_content": "".join(last_lines),
            "message": f"成功读取任务 {task_id} 的日志，共 {len(last_lines)} 行"
        }, ensure_ascii=False, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "message": f"读取日志失败: {str(e)}"
        }, ensure_ascii=False, indent=2)
