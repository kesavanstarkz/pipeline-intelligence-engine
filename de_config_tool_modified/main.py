from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import config, diff, export, browse, extract

# Import all file browser modules to trigger @register decorators
import services.file_browsers

app = FastAPI(
    title="DE Config Management Tool",
    description="Dynamic Data Engineering source config manager with deep diff & reconcile",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(config.router, prefix="/api/config", tags=["Config"])
app.include_router(diff.router, prefix="/api/diff", tags=["Diff"])
app.include_router(export.router, prefix="/api/export", tags=["Export"])
app.include_router(browse.router, prefix="/api/browse", tags=["Browse"])
app.include_router(extract.router, prefix="/api/extract", tags=["Universal Extractor"])
