import logging
import secrets
from typing import Optional

import msal
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import RedirectResponse
from starlette.concurrency import run_in_threadpool
from config.settings import settings

logger = logging.getLogger("pipeline_ie.auth")

router = APIRouter(tags=["Authentication"])
GRAPH_AND_FABRIC_SCOPES = [
    "User.Read",
    "User.ReadBasic.All",
    "Directory.Read.All",
    "https://analysis.windows.net/powerbi/api/.default",
]

def get_msal_app():
    """Build the MSAL confidential client application."""
    if not settings.azure_client_id or not settings.azure_client_secret:
        return None
    
    # Check for placeholder strings to prevent MSAL ValueError
    if "YOUR_" in settings.azure_client_id or "YOUR_" in (settings.azure_tenant_id or ""):
        logger.warning("Azure MSAL auth skipped: Placeholder strings detected in .env")
        return None
        
    # Authority for Microsoft account logins
    authority = f"https://login.microsoftonline.com/{settings.azure_tenant_id or 'common'}"
    
    return msal.ConfidentialClientApplication(
        settings.azure_client_id,
        authority=authority,
        client_credential=settings.azure_client_secret
    )

@router.get("/login")
async def login(request: Request):
    """Initiate the login flow by redirecting to Microsoft."""
    app = get_msal_app()
    if not app:
        return RedirectResponse(url="/?error=Azure+Auth+not+configured+in+.env")
        
    # Generate the auth URL
    # Scope for Graph API to get user info and also for scanning if desired
    scopes = GRAPH_AND_FABRIC_SCOPES
    auth_state = secrets.token_urlsafe(24)
    request.session["auth_state"] = auth_state
    
    auth_url = app.get_authorization_request_url(
        scopes,
        redirect_uri=settings.azure_redirect_uri,
        state=auth_state,
    )
    
    return RedirectResponse(auth_url)

@router.get("/getAToken")
async def get_a_token(request: Request, code: Optional[str] = None, state: Optional[str] = None):
    """Callback from Microsoft to exchange code for token."""
    if not code:
        raise HTTPException(status_code=400, detail="Authorization code missing")
    expected_state = request.session.pop("auth_state", None)
    if expected_state and state != expected_state:
        logger.error("MSAL callback state mismatch: expected=%s actual=%s", expected_state, state)
        raise HTTPException(status_code=401, detail="Authentication failed: invalid OAuth state")
        
    app = get_msal_app()
    if not app:
        raise HTTPException(status_code=500, detail="Azure Auth not configured.")
        
    # Exchange code for tokens
    result = app.acquire_token_by_authorization_code(
        code,
        scopes=GRAPH_AND_FABRIC_SCOPES,
        redirect_uri=settings.azure_redirect_uri
    )
    
    if "error" in result:
        logger.error(f"Auth failed: {result.get('error_description')}")
        raise HTTPException(status_code=401, detail=result.get("error_description"))
        
    # Store user info and tokens in session
    request.session["user"] = result.get("id_token_claims")
    request.session["token_cache"] = app.serializable_token_cache.serialize()
    
    if "access_token" in result:
        request.session["access_token"] = result["access_token"]

    # ARM and Fabric/Power BI are different token audiences — acquire Fabric token separately.
    fabric_scopes = ["https://analysis.windows.net/powerbi/api/.default"]
    account = result.get("account")
    if not account and app.get_accounts():
        account = app.get_accounts()[0]
    if account:
        fb = app.acquire_token_silent(fabric_scopes, account=account)
        if fb and "access_token" in fb:
            request.session["access_token_fabric"] = fb["access_token"]
        else:
            logger.warning(
                "Silent Fabric token not obtained (consent or policy). "
                "Microsoft Fabric scan may return 401 until Power BI scope is granted."
            )

    return RedirectResponse(url="/")

@router.get("/browser-login")
async def browser_login(request: Request):
    """
    Direct Interactive SSO Login.
    Opens a browser on the local machine to log in via Azure SDK's well-known client.
    Does not require AZURE_CLIENT_ID in settings.

    Only one interactive token is acquired (Fabric / Power BI scope). A second
    ``get_token`` for ARM starts another OAuth flow and causes **state mismatch**
    errors with InteractiveBrowserCredential. For Azure Resource Manager use
    Service Principal in .env or the ``/login`` redirect flow.
    """
    try:
        from azure.identity import InteractiveBrowserCredential

        if settings.azure_tenant_id:
            credential = InteractiveBrowserCredential(tenant_id=settings.azure_tenant_id)
        else:
            credential = InteractiveBrowserCredential()

        t_fabric = await run_in_threadpool(
            credential.get_token, "https://analysis.windows.net/powerbi/api/.default"
        )
        request.session["access_token_fabric"] = t_fabric.token

        request.session["user"] = {
            "name": "Azure Portal User",
            "preferred_username": "Interactive SSO (Fabric scope)",
            "auth_mode": "interactive_browser",
        }

        return RedirectResponse(url="/?success=Logged+in+via+Browser+SSO+Fabric")

    except Exception as e:
        error_text = str(e)
        if "state mismatch" in error_text.lower():
            logger.warning(
                "Interactive browser auth state mismatch detected; retrying with device code flow"
            )
            try:
                from azure.identity import DeviceCodeCredential

                def _prompt_callback(verification_uri: str, user_code: str, expires_on: object) -> None:
                    logger.warning(
                        "Device code login required. Open %s and enter code %s before %s",
                        verification_uri,
                        user_code,
                        expires_on,
                    )

                device_credential = DeviceCodeCredential(
                    tenant_id=settings.azure_tenant_id or None,
                    prompt_callback=_prompt_callback,
                )
                t_fabric = await run_in_threadpool(
                    device_credential.get_token, "https://analysis.windows.net/powerbi/api/.default"
                )
                request.session["access_token_fabric"] = t_fabric.token
                request.session["user"] = {
                    "name": "Azure Portal User",
                    "preferred_username": "Device Code (Fabric scope)",
                    "auth_mode": "device_code",
                }
                return RedirectResponse(url="/?success=Logged+in+via+Device+Code+Fabric")
            except Exception as fallback_error:
                logger.error("Device code fallback failed: %s", fallback_error)
                return RedirectResponse(
                    url=f"/?error=Browser+login+failed:+{fallback_error}"
                )

        logger.error("Browser login failed: %s", error_text)
        return RedirectResponse(url=f"/?error=Browser+login+failed:+{error_text}")

@router.get("/logout")
async def logout(request: Request):
    """Clear the session."""
    request.session.clear()
    return RedirectResponse(url="/")
