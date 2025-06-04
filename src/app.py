from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers.process_dataset import process_dataset_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_headers=['*'],
    allow_methods=["POST"],
)

app.include_router(process_dataset_router)
