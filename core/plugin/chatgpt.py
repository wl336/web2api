"""
ChatGPT 插件：实现 chatgpt.com 的站点上下文获取、会话状态初始化、请求体构建、SSE 解析与限流处理。
"""

import base64
import datetime
import json
import logging
import time
import uuid
from typing import Any

from playwright.async_api import BrowserContext, Page

from core.api.schemas import InputAttachment
from core.plugin.base import BaseSitePlugin, PluginRegistry, SiteConfig
from core.plugin.helpers import request_json_via_page_fetch

logger = logging.getLogger(__name__)


async def _put_file_to_presigned_url(
    page: Page,
    upload_url: str,
    *,
    data: bytes,
    mime_type: str,
    timeout_ms: int = 30000,
) -> dict[str, Any]:
    """使用预签名 URL 直接 PUT 二进制文件。"""
    result = await page.evaluate(
        """
async ({ uploadUrl, dataBase64, mimeType, timeoutMs }) => {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs || 30000);
  try {
    const binary = atob(dataBase64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
    const resp = await fetch(uploadUrl, {
      method: "PUT",
      body: bytes,
      headers: { "Content-Type": mimeType || "application/octet-stream" },
      signal: ctrl.signal,
    });
    clearTimeout(t);
    const text = await resp.text();
    return { ok: resp.ok, status: resp.status, text };
  } catch (e) {
    clearTimeout(t);
    const msg = e.name === "AbortError" ? `请求超时(${Math.floor((timeoutMs || 30000) / 1000)}s)` : (e.message || String(e));
    return { error: msg };
  }
}
        """,
        {
            "uploadUrl": upload_url,
            "dataBase64": base64.b64encode(data).decode("ascii"),
            "mimeType": mime_type,
            "timeoutMs": timeout_ms,
        },
    )
    if not isinstance(result, dict):
        raise RuntimeError("ChatGPT 文件上传返回异常")
    if result.get("error"):
        raise RuntimeError(str(result["error"]))
    return result


class ChatGPTPlugin(BaseSitePlugin):
    """ChatGPT Web2API 插件。auth 建议包含 __Secure-next-auth.session-token。"""

    type_name = "chatgpt"

    site = SiteConfig(
        start_url="https://chatgpt.com",
        api_base="https://chatgpt.com/backend-api",
        cookie_name="__Secure-next-auth.session-token",
        cookie_domain=".chatgpt.com",
        auth_keys=[
            "__Secure-next-auth.session-token",
            "session-token",
            "session_token",
            "accessToken",
        ],
        config_section="chatgpt",
    )

    async def fetch_site_context(
        self, context: BrowserContext, page: Page
    ) -> dict[str, Any] | None:
        del context
        resp = await request_json_via_page_fetch(
            page,
            f"{self.api_base}/me",
            timeout_ms=15000,
        )
        if int(resp.get("status") or 0) != 200:
            text = str(resp.get("text") or "")[:500]
            logger.warning(
                "[%s] fetch_site_context 失败 status=%s url=%s body=%s",
                self.type_name,
                resp.get("status"),
                resp.get("url"),
                text,
            )
            return None
        data = resp.get("json")
        if not isinstance(data, dict):
            logger.warning("[%s] fetch_site_context 返回非 JSON", self.type_name)
            return None
        account_id = data.get("id") or data.get("account_id")
        return {"account_id": str(account_id)} if account_id else {}

    async def create_session(
        self,
        context: BrowserContext,
        page: Page,
        site_context: dict[str, Any],
    ) -> str | None:
        del context, page, site_context
        # ChatGPT 对话通常在第一次 completion 时创建，因此此处初始化本地会话 ID 即可。
        return str(uuid.uuid4())

    def build_completion_url(self, session_id: str, state: dict[str, Any]) -> str:
        del session_id, state
        return f"{self.api_base}/conversation"

    def build_completion_body(
        self,
        message: str,
        session_id: str,
        state: dict[str, Any],
        prepared_attachments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        parent_message_id = state.get("parent_message_id") or str(uuid.uuid4())
        conversation_id = state.get("conversation_id")

        body: dict[str, Any] = {
            "action": "next",
            "messages": [
                {
                    "id": str(uuid.uuid4()),
                    "author": {"role": "user"},
                    "content": {"content_type": "text", "parts": [message]},
                    "metadata": {},
                }
            ],
            "parent_message_id": parent_message_id,
            "model": state.get("model") or "auto",
            "history_and_training_disabled": False,
            "websocket_request_id": session_id,
        }
        if conversation_id:
            body["conversation_id"] = conversation_id
        if prepared_attachments:
            body.update(prepared_attachments)
        return body

    def parse_stream_event(
        self,
        payload: str,
    ) -> tuple[list[str], str | None, str | None]:
        texts: list[str] = []
        message_id: str | None = None
        error_message: str | None = None
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            return (texts, message_id, error_message)

        if not isinstance(obj, dict):
            return (texts, message_id, error_message)

        if obj.get("error"):
            error_message = str(obj.get("error"))
            return (texts, message_id, error_message)

        conversation_id = obj.get("conversation_id")
        if conversation_id:
            message_id = f"conversation:{conversation_id}"

        msg = obj.get("message")
        if isinstance(msg, dict):
            mid = msg.get("id")
            if mid:
                message_id = str(mid)
            content = msg.get("content")
            if isinstance(content, dict):
                parts = content.get("parts")
                if isinstance(parts, list):
                    for part in parts:
                        if isinstance(part, str) and part:
                            texts.append(part)

        # 某些响应会把增量放在 delta 字段。
        delta = obj.get("delta")
        if isinstance(delta, dict):
            dtext = delta.get("text")
            if isinstance(dtext, str) and dtext:
                texts.append(dtext)

        return (texts, message_id, error_message)

    def is_stream_end_event(self, payload: str) -> bool:
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            return False
        if not isinstance(obj, dict):
            return False
        msg = obj.get("message")
        if isinstance(msg, dict):
            status = str(msg.get("status") or "").lower()
            if status in {"finished_successfully", "finished"}:
                return True
            if msg.get("end_turn") is True:
                return True
        return False

    def on_stream_completion_finished(
        self,
        session_id: str,
        message_ids: list[str],
    ) -> None:
        state = self._session_state.get(session_id)
        if not state:
            return
        for m in reversed(message_ids):
            if m.startswith("conversation:"):
                state["conversation_id"] = m.split(":", 1)[1]
                break
        for m in reversed(message_ids):
            if not m.startswith("conversation:"):
                state["parent_message_id"] = m
                break

    def on_http_error(
        self,
        message: str,
        headers: dict[str, str] | None,
    ) -> int | None:
        msg = (message or "").lower()
        overloaded = "429" in msg or "overloaded" in msg or "capacity" in msg
        if not overloaded:
            return None

        if headers:
            retry_after = headers.get("retry-after") or headers.get("Retry-After")
            if retry_after:
                raw = str(retry_after).strip()
                if raw.isdigit():
                    return int(time.time()) + int(raw)
                try:
                    dt = datetime.datetime.strptime(raw, "%a, %d %b %Y %H:%M:%S GMT")
                    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())
                except Exception:
                    pass
        return int(time.time()) + 30 * 60

    async def prepare_attachments(
        self,
        context: BrowserContext,
        page: Page,
        session_id: str,
        state: dict[str, Any],
        attachments: list[InputAttachment],
    ) -> dict[str, Any]:
        del context, session_id, state
        if not attachments:
            return {}

        prepared: list[dict[str, Any]] = []
        for attachment in attachments:
            create_resp = await request_json_via_page_fetch(
                page,
                f"{self.api_base}/files",
                method="POST",
                body=json.dumps(
                    {
                        "file_name": attachment.filename,
                        "file_size": len(attachment.data),
                        "use_case": "multimodal",
                    }
                ),
                headers={"Content-Type": "application/json"},
                timeout_ms=30000,
            )
            status = int(create_resp.get("status") or 0)
            if status not in (200, 201):
                text = str(create_resp.get("text") or "")[:500]
                raise RuntimeError(f"ChatGPT 创建上传任务失败 {status}: {text}")
            info = create_resp.get("json")
            if not isinstance(info, dict):
                raise RuntimeError("ChatGPT 创建上传任务返回非 JSON")

            file_id = info.get("file_id") or info.get("id")
            upload_url = info.get("upload_url")
            if not file_id or not upload_url:
                raise RuntimeError("ChatGPT 上传任务缺少 file_id 或 upload_url")

            put_resp = await _put_file_to_presigned_url(
                page,
                str(upload_url),
                data=attachment.data,
                mime_type=attachment.mime_type,
            )
            if int(put_resp.get("status") or 0) not in (200, 201):
                text = str(put_resp.get("text") or "")[:500]
                raise RuntimeError(
                    f"ChatGPT 直传文件失败 {put_resp.get('status')}: {text}"
                )

            done_resp = await request_json_via_page_fetch(
                page,
                f"{self.api_base}/files/{file_id}/uploaded",
                method="POST",
                headers={"Content-Type": "application/json"},
                body=json.dumps({}),
                timeout_ms=20000,
            )
            done_status = int(done_resp.get("status") or 0)
            if done_status not in (200, 201, 204):
                text = str(done_resp.get("text") or "")[:500]
                raise RuntimeError(f"ChatGPT 确认上传失败 {done_status}: {text}")

            prepared.append(
                {
                    "id": str(file_id),
                    "name": attachment.filename,
                    "mime_type": attachment.mime_type,
                    "size": len(attachment.data),
                }
            )

        return {"attachments": prepared}


def register_chatgpt_plugin() -> None:
    """注册 ChatGPT 插件到全局 Registry。"""
    PluginRegistry.register(ChatGPTPlugin())
