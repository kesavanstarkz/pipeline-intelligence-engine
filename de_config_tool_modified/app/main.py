from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path

from app.routers import config, diff, reconcile

BASE_DIR = Path(__file__).resolve().parent.parent

app = FastAPI(
    title="DE Config Management Tool",
    description="Dynamic Data Engineering source config generator, diff engine, and reconciler",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app.include_router(config.router, prefix="/api/config", tags=["Config"])
app.include_router(diff.router, prefix="/api/diff", tags=["Diff"])
app.include_router(reconcile.router, prefix="/api/reconcile", tags=["Reconcile"])

from fastapi import Request
from fastapi.responses import HTMLResponse

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
