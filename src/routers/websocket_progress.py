from fastapi import APIRouter, WebSocket

from ..websocket_manager import active_websockets

websocket_router = APIRouter()


@websocket_router.websocket("/ws/progress/{session_id}")
async def websocket_progress(websocket: WebSocket, session_id: str):
    # print(f"Accepting WebSocket connection for session_id: {session_id}")
    await websocket.accept()
    if session_id not in active_websockets:
        active_websockets[session_id] = []
    active_websockets[session_id].append(websocket)

    try:
        while True:
            await websocket.receive_text()  # Keep connection alive
    except Exception as e:
        # print(f"WebSocket error for session_id {session_id}: {str(e)}")
        active_websockets[session_id].remove(websocket)
        if not active_websockets[session_id]:
            del active_websockets[session_id]
        await websocket.close()
