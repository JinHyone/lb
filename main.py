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
from linebot.v3.webhooks import MessageEvent, TextMessageContent

channel_secret = ''
channel_access_token = ''

configuration = Configuration(
    access_token=channel_access_token
)

app = FastAPI()
async_api_clinet = AsyncApiClient(Configuration)
line_bot_api = AsyncMessagingApi(async_api_clinet)
parser = WebhookParser(channel_secret)



@app.post("/callback")
async def handle_callback(request: Request):
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = await request.body()
    body = body.decode()

    try:
        events = parser.parse(body, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    for event in events: # type: ignore
        if not isinstance(event, MessageEvent):
            continue
        if not isinstance(event.message, TextMessageContent):
            continue


        await line_bot_api.reply_message(
            ReplyMessageRequest(
                replyToken=event.reply_token, # type: ignore
                messages=[TextMessage(text=event.message.text)], # type: ignore
                notificationDisabled=False
            )
        )

    return 'OK'