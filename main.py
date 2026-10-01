import os
import sys
from contextlib import asynccontextmanager

from dotenv import load_dotenv
import uvicorn

from fastapi import Request, FastAPI, HTTPException

from linebot.v3.webhook import WebhookParser
from linebot.v3.messaging import (
    AsyncApiClient,
    AsyncMessagingApi,
    Configuration,
    ReplyMessageRequest,
    TextMessage
)
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import (
    MessageEvent,
    TextMessageContent
)


load_dotenv()

# get channel_secret and channel_access_token from your environment variable
channel_secret = os.getenv("LINE_CHANNEL_SECRET")
channel_access_token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")

if channel_secret is None:
    print("Specify LINE_CHANNEL_SECRET as environment variable.")
    sys.exit(1)

if channel_access_token is None:
    print("Specify LINE_CHANNEL_ACCESS_TOKEN as environment variable.")
    sys.exit(1)


configuration = Configuration(
    access_token=channel_access_token
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # FastAPI/Uvicorn이 event loop를 실행한 후 생성
    async_api_client = AsyncApiClient(configuration)

    app.state.async_api_client = async_api_client
    app.state.line_bot_api = AsyncMessagingApi(async_api_client)

    yield

    # 종료 시 client 정리
    await async_api_client.close()


app = FastAPI(lifespan=lifespan)

parser = WebhookParser(channel_secret)


@app.post("/callback")
async def handle_callback(request: Request):
    signature = request.headers.get("X-Line-Signature")

    if signature is None:
        raise HTTPException(
            status_code=400,
            detail="Missing X-Line-Signature"
        )

    # get request body as text
    body = await request.body()
    body = body.decode("utf-8")

    try:
        events = parser.parse(body, signature)
    except InvalidSignatureError:
        raise HTTPException(
            status_code=400,
            detail="Invalid signature"
        )

    line_bot_api = request.app.state.line_bot_api

    for event in events: # type: ignore
        if not isinstance(event, MessageEvent):
            continue

        if not isinstance(event.message, TextMessageContent):
            continue


        await line_bot_api.reply_message(
            ReplyMessageRequest(
                replyToken=event.reply_token, # type: ignore
                messages=[
                    TextMessage(text=event.message.text) # type: ignore
                ],
                notificationDisabled=False
            )
        )

    return "OK"