"""
System Log API Routes

"""

from fastapi import APIRouter, Query
from typing import Optional, List, Dict, Any
from services.system_logger import system_logger

router = APIRouter(prefix="/api/system-logs", tags=["System Logs"])


@router.get("/")
async def get_system_logs(
    level: Optional[str] = Query(None, description=": INFO, WARNING, ERROR"),
    category: Optional[str] = Query(None, description=": price_update, ai_decision, system_error"),
    limit: int = Query(100, ge=1, le=500, description="")
) -> Dict[str, Any]:
    """
    

    :
    - level:  (INFO, WARNING, ERROR)
    - category:  (price_update, ai_decision, system_error)
    - limit:  (1-500)

    :
    - logs: 
    - total: 
    """
    min_level = None if level else "WARNING"
    logs = system_logger.get_logs(
        level=level,
        category=category,
        limit=limit,
        min_level=min_level,
    )
    return {
        "logs": logs,
        "total": len(logs)
    }


@router.get("/categories")
async def get_log_categories() -> Dict[str, List[str]]:
    """
    

    :
    - categories: 
    - levels: 
    """
    return {
        "categories": ["price_update", "ai_decision", "system_error"],
        "levels": ["INFO", "WARNING", "ERROR"]
    }


@router.delete("/")
async def clear_system_logs() -> Dict[str, str]:
    """
    

    :
    - message: 
    """
    system_logger.clear_logs()
    return {"message": "All system logs cleared successfully"}


@router.get("/stats")
async def get_log_stats() -> Dict[str, Any]:
    """
    

    :
    - total_logs: 
    - by_level: 
    - by_category: 
    """
    all_logs = system_logger.get_logs(limit=500, min_level="WARNING")

    stats = {
        "total_logs": len(all_logs),
        "by_level": {
            "INFO": 0,
            "WARNING": 0,
            "ERROR": 0
        },
        "by_category": {
            "price_update": 0,
            "ai_decision": 0,
            "system_error": 0
        }
    }

    for log in all_logs:
        level = log.get("level", "INFO")
        category = log.get("category", "system_error")

        if level in stats["by_level"]:
            stats["by_level"][level] += 1
        if category in stats["by_category"]:
            stats["by_category"][category] += 1

    return stats
