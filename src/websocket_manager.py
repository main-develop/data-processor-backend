from fastapi import WebSocket
from typing import Dict, List

active_websockets: Dict[str, List[WebSocket]] = {}

async def websocket_send_progress(session_id: str, progress: float, message: str):
    websockets = active_websockets.get(session_id, [])
    for websocket in websockets:
        try:
            await websocket.send_json({"progress": progress, "message": message})
        except Exception:
            # Remove closed WebSocket
            websockets.remove(websocket)
