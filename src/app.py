from dask.distributed import Client, LocalCluster
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers.analyze_dataset import analyze_dataset_router
from .routers.process_dataset import process_dataset_router
from .routers.websocket_progress import websocket_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_headers=['*'],
    allow_methods=['*'],
    allow_credentials=True,
)

app.include_router(process_dataset_router)
app.include_router(analyze_dataset_router)
app.include_router(websocket_router)

cluster = LocalCluster(n_workers=4)
client = Client(cluster)
