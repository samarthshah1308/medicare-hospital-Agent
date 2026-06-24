import os
from fastapi import WebSocket
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask
from pipecat.transports.network.fastapi_websocket import FastAPIWebsocketTransport
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.openai.llm import OpenAILLMService

# IMPORT YOUR SAME EXISTING LOGIC
from .services import handle_hospital_voice_flow


async def run_voice_agent(websocket: WebSocket):
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params={
            "audio_in_enabled": True,
            "audio_out_enabled": True,
            "vad_enabled": True,
        },
    )

    stt = DeepgramSTTService(api_key=os.getenv("DEEPGRAM_API_KEY"))

    llm = OpenAILLMService(
        api_key=os.getenv("OPENAI_API_KEY"),
        model="gpt-4o-mini",
    )

    tts = CartesiaTTSService(api_key=os.getenv("CARTESIA_API_KEY"))

    pipeline = Pipeline([
        transport.input(),
        stt,
        llm,
        handle_hospital_voice_flow,
        tts,
        transport.output(),
    ])

    task = PipelineTask(pipeline)
    runner = PipelineRunner()
    await runner.run(task)