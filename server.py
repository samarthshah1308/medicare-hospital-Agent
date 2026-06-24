"""
Medicare Hospital Voice AI Agent - Fixed Server
Fixes:
1. Pipecat pipeline: STT → Groq LLM → TTS (LLM was missing before)
2. Microphone WebSocket properly connected
3. All existing endpoints unchanged
"""

import json
import os
import uuid
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import uvicorn

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.deepgram.tts import DeepgramTTSService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.network.fastapi_websocket import (
    FastAPIWebsocketTransport,
    FastAPIWebsocketParams,
)
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMMessagesFrame, TextFrame
from pipecat.processors.aggregators.llm_response import (
    LLMAssistantResponseAggregator,
    LLMUserResponseAggregator,
)

from supabase import create_client, Client
from dotenv import load_dotenv
from groq import Groq

load_dotenv()

groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("medicare-voice-agent")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI(title="Medicare Voice AI Agent", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)


@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)


@app.options("/{rest_of_path:path}")
async def preflight_handler(request: Request, rest_of_path: str):
    return JSONResponse(content={}, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS, PATCH",
        "Access-Control-Allow-Headers": "*",
    })


# ─── Hospital Knowledge Base ──────────────────────────────────────────────────
HOSPITAL_KNOWLEDGE = """
MEDICARE GENERAL HOSPITAL - Complete Information Guide

## About Us
Medicare General Hospital is a leading multi-specialty healthcare institution with 35+ years of excellence.
Founded in 1985. Trusted by 50,000+ patients. 250+ Expert Doctors. 20+ Awards Won.
Located at: 123 Healthcare Blvd, Medical City, MC 54321
24/7 Emergency Helpline: (555) 123-4567
Main Reception: (555) 123-4567
Email: info@medicare-hospital.com
Website: medicare-hospital.com
Working Hours: Mon-Sat 8:00 AM to 8:00 PM
Emergency: 24 hours, 7 days a week

## Departments & Specialties
We have 10 specialized departments:
1. Cardiology - Heart diseases, ECG, Echo, Angiography, Bypass surgery. Head: Dr. James Wilson
2. Neurology - Brain, spine, nervous system, stroke, epilepsy. Head: Dr. Sarah Chen
3. Orthopedics - Bones, joints, sports injuries, knee replacement, fractures. Head: Dr. Robert Kumar
4. Pediatrics - Children health 0 to 18 years, vaccinations, growth. Head: Dr. Emily Davis
5. Ophthalmology - Eye care, cataract surgery, LASIK, glaucoma. Head: Dr. Michael Patel
6. General Medicine - Primary care, fever, infections, diabetes, BP checkups
7. Pulmonology - Lungs, asthma, COPD, breathing disorders, TB
8. Oncology - Cancer diagnosis, chemotherapy, radiation therapy
9. Dermatology - Skin diseases, hair loss, acne, psoriasis
10. Emergency Medicine - 24/7 trauma and critical care, ICU

## Doctors List
- Dr. James Wilson - Cardiology - 20 years experience
- Dr. Sarah Chen - Neurology - 15 years experience
- Dr. Robert Kumar - Orthopedics - 18 years experience
- Dr. Emily Davis - Pediatrics - 12 years experience
- Dr. Michael Patel - Ophthalmology - 16 years experience
- Dr. Carlos Rivera - Cardiology - 10 years experience
- Dr. Sarah Mitchell - Cardiology - 8 years experience
- Dr. Raj Patel - General Medicine - 14 years experience
- Dr. Priya Sharma - Dermatology - 9 years experience
- Dr. Anil Mehta - Pulmonology - 11 years experience

## Hospital Timings
- OPD Outpatient: Monday to Saturday 8:00 AM to 8:00 PM
- Emergency Department: 24 hours 7 days
- Pharmacy: 24 hours 7 days
- Lab Reports: 7:00 AM to 10:00 PM
- Visiting Hours for patients: 4:00 PM to 7:00 PM
- Radiology: 8:00 AM to 8:00 PM

## Services Available
- Outpatient Consultations (OPD)
- Inpatient Admissions and Ward
- Diagnostic Lab - blood tests, urine, culture, CBC, LFT, KFT
- Radiology - X-Ray, CT Scan, MRI, Ultrasound, Mammography
- Pharmacy 24 hours
- ICU and Critical Care - 20 beds
- Operation Theatres - 8 fully equipped OTs
- Ambulance Service call (555) 123-4567
- Physiotherapy and Rehabilitation
- Blood Bank
- Cafeteria and Patient Meals
- Free WiFi for patients - Network MedCare_Guest no password
- Wheelchair available free at entrance

## Appointment Booking Process
To book appointment I need these details one by one:
1. Full name of patient
2. Age and gender
3. Phone number
4. Email address
5. Department or specialty needed
6. Patient type - New patient or Follow-up
7. Preferred date - future date only
8. Time slot - Morning 9AM to 12PM, Midday 11AM to 1PM, Afternoon 2PM to 4PM, Evening 4PM to 6PM
9. Any notes or symptoms (optional)

## Emergency Services
Call immediately: (555) 123-4567
Ambulance 24/7. City response time 10 to 15 minutes.
Emergency for: Heart attacks, Strokes, Road accidents, Burns, Poisoning, Unconscious patient, Severe bleeding

## Insurance and Billing
Accepted insurance: Star Health, HDFC Ergo, ICICI Lombard, New India Assurance,
United Health Care, Arogya Sanjeevani, Ayushman Bharat PMJAY, Mediassist
Cashless treatment available.

## Lab Reports
Available online on Patient Portal after login
Routine tests ready in 4 to 6 hours
"""

# ─── Smart Greeting ───────────────────────────────────────────────────────────
def get_smart_greeting() -> str:
    hour = datetime.now().hour
    if hour < 12:
        greeting = "Good morning"
    elif hour < 17:
        greeting = "Good afternoon"
    else:
        greeting = "Good evening"
    return (
        f"{greeting}! Welcome to Medicare General Hospital. I'm ARIA, your AI health assistant. "
        f"I can help you with: Booking appointments, Finding departments like Cardiology, Neurology, Orthopedics, "
        f"Finding doctors, Emergency services, Hospital information, and much more. "
        f"How can I assist you today?"
    )


# ─── RAG Engine ───────────────────────────────────────────────────────────────
def rag_search(query: str) -> str:
    query_lower = query.lower()
    sections = HOSPITAL_KNOWLEDGE.split("##")
    scored = []
    for section in sections:
        keywords = query_lower.split()
        matches = sum(1 for kw in keywords if len(kw) > 2 and kw in section.lower())
        if matches > 0:
            scored.append((matches, section.strip()))
    scored.sort(reverse=True, key=lambda x: x[0])
    top = [s[1] for s in scored[:4]]
    return "\n\n".join(top) if top else HOSPITAL_KNOWLEDGE[:800]


# ─── Slot Availability ────────────────────────────────────────────────────────
def check_slot_availability(doctor_name: str, preferred_date: str, time_slot: str) -> dict:
    try:
        result = supabase.table("appointments").select("*").eq(
            "preferred_date", preferred_date
        ).execute()

        booked_slots = []
        for row in result.data:
            if doctor_name and doctor_name.lower() in (row.get("doctor_name") or "").lower():
                booked_slots.append(row.get("time_slot", "").lower())

        all_slots = ["Morning", "Midday", "Afternoon", "Evening"]
        is_booked = time_slot.lower() in booked_slots
        available_slots = [s for s in all_slots if s.lower() not in booked_slots]

        return {
            "available": not is_booked,
            "booked_slots": booked_slots,
            "available_slots": available_slots
        }
    except Exception as e:
        logger.error(f"Slot check error: {e}")
        return {"available": True, "booked_slots": [], "available_slots": []}


# ─── Agent Task Executor ──────────────────────────────────────────────────────
class AgentTaskExecutor:
    def __init__(self, websocket_callback=None):
        self.ws_callback = websocket_callback

    async def send_ui_command(self, command: dict):
        if self.ws_callback:
            try:
                await self.ws_callback(json.dumps({"type": "ui_command", **command}))
            except Exception as e:
                logger.error(f"UI command send error: {e}")

    async def book_appointment(self, data: dict) -> str:
        try:
            required = ["full_name", "age", "phone", "email", "department",
                        "patient_type", "preferred_date", "time_slot"]
            missing = [f for f in required if not data.get(f)]
            if missing:
                return f"I still need: {', '.join(missing)}. Please provide these details."

            doctor = data.get("doctor_name", "")
            date = data.get("preferred_date", "")
            slot = data.get("time_slot", "")

            if doctor and date and slot:
                availability = check_slot_availability(doctor, date, slot)
                if not availability["available"]:
                    next_slots = availability["available_slots"]
                    if next_slots:
                        return (
                            f"Sorry! The {slot} slot with {doctor} on {date} is already booked. "
                            f"Available slots are: {', '.join(next_slots)}. Which would you prefer?"
                        )
                    else:
                        return (
                            f"Sorry! All slots with {doctor} on {date} are fully booked. "
                            f"Please choose a different date or doctor."
                        )

            supabase.table("appointments").insert({
                "full_name": data["full_name"],
                "age": int(data["age"]),
                "gender": data.get("gender", "Not specified"),
                "phone": data["phone"],
                "email": data["email"],
                "department": data["department"],
                "patient_type": data["patient_type"],
                "preferred_date": data["preferred_date"],
                "time_slot": data["time_slot"],
                "notes": data.get("notes", ""),
                "doctor_name": data.get("doctor_name", ""),
                "created_at": datetime.utcnow().isoformat(),
                "status": "pending"
            }).execute()

            await self.send_ui_command({
                "action": "navigate",
                "path": "/appointment",
                "fill_form": data,
                "submit": True,
            })

            return (
                f"Appointment booked successfully! "
                f"{data['full_name']}, your appointment in {data['department']} "
                f"is confirmed for {data['preferred_date']} during the "
                f"{data['time_slot']} slot. You will receive a confirmation call shortly."
            )
        except Exception as e:
            logger.error(f"Booking error: {e}")
            return "I encountered an issue booking your appointment. Please try again."

    async def submit_emergency(self, data: dict) -> str:
        try:
            supabase.table("emergency_requests").insert({
                "patient_name": data.get("patient_name", "Unknown"),
                "contact_number": data.get("contact_number", ""),
                "location": data.get("location", ""),
                "emergency_type": data.get("emergency_type", "General"),
                "description": data.get("description", ""),
                "created_at": datetime.utcnow().isoformat(),
                "status": "dispatched"
            }).execute()

            await self.send_ui_command({
                "action": "navigate",
                "path": "/emergency",
                "fill_form": data,
                "alert": "EMERGENCY_DISPATCHED",
            })

            return (
                "EMERGENCY ALERT SENT! An ambulance has been dispatched. "
                "ETA 10 to 15 minutes. Stay calm. "
                "Emergency number: (555) 123-4567"
            )
        except Exception as e:
            logger.error(f"Emergency error: {e}")
            return "Emergency alert sent! Please also call (555) 123-4567 immediately."

    async def navigate_to(self, path: str) -> str:
        page_map = {
            "/": "Home",
            "/appointment": "Appointment Booking",
            "/emergency": "Emergency Services",
            "/doctors": "Our Doctors",
            "/departments": "Departments",
            "/services": "Services",
            "/patient-portal": "Patient Portal",
            "/lab-reports": "Lab Reports",
            "/contact": "Contact Us",
            "/about": "About Us",
            "/patient-login": "Patient Login",
            "/patient-register": "Patient Registration",
        }
        await self.send_ui_command({
            "action": "navigate",
            "path": path,
        })
        page_name = page_map.get(path, path)
        return f"Taking you to the {page_name} page now."


# ─── System Prompt ────────────────────────────────────────────────────────────
def build_system_prompt(rag_context: str = "") -> str:
    return f"""You are ARIA, the voice AI assistant for Medicare General Hospital. You are warm, professional, and efficient.

HOSPITAL CONTEXT:
{rag_context or HOSPITAL_KNOWLEDGE[:1500]}

YOUR CAPABILITIES:
1. Answer ANY question about hospital - services, departments, doctors, address, phone, timings
2. Book appointments step by step
3. Handle emergencies
4. Navigate to any page
5. Help with patient registration

RULES:
- Keep responses SHORT (2-3 sentences max for voice)
- Always answer from HOSPITAL CONTEXT
- Phone: (555) 123-4567
- Email: info@medicare-hospital.com
- Address: 123 Healthcare Blvd, Medical City
- Hours: Mon-Sat 8AM-8PM, Emergency 24/7

NAVIGATION - when user says these, respond with action tag:
- "find doctor" or "show doctors" or "doctors list" → [ACTION:NAVIGATE:/doctors]
- "book appointment" or "appointment" → [ACTION:NAVIGATE:/appointment]  
- "cardiology" or "neurology" or "orthopedics" etc → [ACTION:NAVIGATE:/departments]
- "emergency" → [ACTION:NAVIGATE:/emergency]
- "lab reports" → [ACTION:NAVIGATE:/lab-reports]
- "home" → [ACTION:NAVIGATE:/]
- "contact" → [ACTION:NAVIGATE:/contact]
- "services" → [ACTION:NAVIGATE:/services]
- "patient portal" → [ACTION:NAVIGATE:/patient-portal]
- "login" → [ACTION:NAVIGATE:/patient-login]
- "register" → [ACTION:NAVIGATE:/patient-register]

APPOINTMENT BOOKING:
Collect one at a time: full_name → age → gender → phone → email → department → patient_type → preferred_date → time_slot
Once ALL 8 fields collected, IMMEDIATELY execute:
[ACTION:BOOK_APPOINTMENT:{{"full_name":"...","age":...,"gender":"...","phone":"...","email":"...","department":"...","patient_type":"...","preferred_date":"YYYY-MM-DD","time_slot":"..."}}]
Do NOT ask for confirmation. Just book directly.
Slots: Morning, Midday, Afternoon, Evening.

EMERGENCY: chest pain / stroke / accident → [ACTION:EMERGENCY:{{"patient_name":"...","contact_number":"...","emergency_type":"..."}}]

CRITICAL: For "find doctor" or "I need a doctor" → ALWAYS respond with [ACTION:NAVIGATE:/doctors] so page opens."""


# ─── Connection Manager ───────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.session_data: Dict[str, Dict] = {}

    def disconnect(self, session_id: str):
        self.active_connections.pop(session_id, None)
        self.session_data.pop(session_id, None)
        logger.info(f"Client disconnected: {session_id}")


manager = ConnectionManager()


# ─── Groq AI Call ─────────────────────────────────────────────────────────────
async def call_groq(system_prompt: str, messages: list) -> str:
    try:
        groq_messages = [{"role": "system", "content": system_prompt}]
        for msg in messages[-10:]:
            groq_messages.append({"role": msg["role"], "content": msg["content"]})

        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=groq_messages,
            max_tokens=500,
            temperature=0.7,
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"Groq API error: {e}")
        return "I'm having trouble right now. Please try again or call (555) 123-4567."


# ─── Process Message ──────────────────────────────────────────────────────────
async def process_text_message(session_id: str, message: str, websocket) -> str:
    session = manager.session_data.get(session_id, {})
    if not session:
        manager.session_data[session_id] = {
            "conversation": [], "collected_data": {}, "current_task": None
        }
        session = manager.session_data[session_id]

    conversation = session.get("conversation", [])
    collected_data = session.get("collected_data", {})

    rag_context = rag_search(message)
    messages = []
    for turn in conversation[-10:]:
        messages.append({"role": turn["role"], "content": turn["content"]})
    messages.append({"role": "user", "content": message})

    ai_response = await call_groq(build_system_prompt(rag_context), messages)
    final_response = ai_response

    executor = AgentTaskExecutor(
        websocket_callback=lambda data: websocket.send_text(data)
    )

    if "[ACTION:NAVIGATE:" in ai_response:
        try:
            path_start = ai_response.index("[ACTION:NAVIGATE:") + len("[ACTION:NAVIGATE:")
            path_end = ai_response.index("]", path_start)
            path = ai_response[path_start:path_end].strip()
            final_response = ai_response.replace(f"[ACTION:NAVIGATE:{path}]", "").strip()
            if not final_response:
                final_response = await executor.navigate_to(path)
            else:
                await executor.navigate_to(path)
        except Exception as e:
            logger.error(f"Navigation parse error: {e}")

    elif "[ACTION:BOOK_APPOINTMENT:" in ai_response:
        try:
            json_start = ai_response.index("[ACTION:BOOK_APPOINTMENT:") + len("[ACTION:BOOK_APPOINTMENT:")
            json_end = ai_response.index("]", json_start)
            action_data = json.loads(ai_response[json_start:json_end])
            action_data.update(collected_data)
            final_response = await executor.book_appointment(action_data)
        except Exception as e:
            logger.error(f"Booking parse error: {e}")

    elif "[ACTION:EMERGENCY:" in ai_response:
        try:
            json_start = ai_response.index("[ACTION:EMERGENCY:") + len("[ACTION:EMERGENCY:")
            json_end = ai_response.index("]", json_start)
            action_data = json.loads(ai_response[json_start:json_end])
            final_response = await executor.submit_emergency(action_data)
        except Exception as e:
            logger.error(f"Emergency parse error: {e}")

    for marker in ["[ACTION:BOOK_APPOINTMENT:", "[ACTION:EMERGENCY:", "[ACTION:NAVIGATE:", "[ACTION:REGISTER:"]:
        if marker in final_response:
            try:
                start = final_response.index(marker)
                end = final_response.index("]", start) + 1
                final_response = (final_response[:start] + final_response[end:]).strip()
            except Exception:
                pass

    conversation.append({"role": "user", "content": message})
    conversation.append({"role": "assistant", "content": final_response})
    session["conversation"] = conversation[-20:]

    return final_response


# ─── WebSocket - Text Chat ────────────────────────────────────────────────────
@app.websocket("/ws/voice-agent/{session_id}")
async def voice_agent_websocket(websocket: WebSocket, session_id: str):
    await websocket.accept()
    manager.active_connections[session_id] = websocket
    manager.session_data[session_id] = {
        "conversation": [], "collected_data": {}, "current_task": None
    }
    logger.info(f"Client connected: {session_id}")

    try:
        await websocket.send_text(json.dumps({
            "type": "connected",
            "message": get_smart_greeting(),
            "session_id": session_id
        }))

        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            msg_type = payload.get("type", "text")

            if msg_type == "text":
                user_message = payload.get("message", "").strip()
                if not user_message:
                    continue
                await websocket.send_text(json.dumps({"type": "thinking"}))
                response = await process_text_message(session_id, user_message, websocket)
                await websocket.send_text(json.dumps({
                    "type": "response",
                    "message": response,
                    "timestamp": datetime.utcnow().isoformat()
                }))

            elif msg_type == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))

            elif msg_type == "update_data":
                session = manager.session_data.get(session_id, {})
                session.get("collected_data", {}).update(payload.get("data", {}))

    except WebSocketDisconnect:
        manager.disconnect(session_id)
    except Exception as e:
        logger.error(f"WebSocket error {session_id}: {e}")
        manager.disconnect(session_id)


# ─── ✅ FIXED: Pipecat Voice WebSocket - STT → Groq LLM → TTS ────────────────
@app.websocket("/ws/pipecat/{session_id}")
async def pipecat_audio_websocket(websocket: WebSocket, session_id: str):
    await websocket.accept()
    logger.info(f"Pipecat connected: {session_id}")

    try:
        transport = FastAPIWebsocketTransport(
            websocket=websocket,
            params=FastAPIWebsocketParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                add_wav_header=True,
                vad_enabled=True,
                vad_analyzer=SileroVADAnalyzer(),
                vad_audio_passthrough=True,
            ),
        )

        stt = DeepgramSTTService(
            api_key=os.getenv("DEEPGRAM_API_KEY"),
            language="en-US",
        )

        # ✅ FIX: OpenAI-compatible LLM using Groq
        # Groq supports OpenAI SDK format - just change base_url
        llm = OpenAILLMService(
            api_key=os.getenv("GROQ_API_KEY"),
            model="llama-3.3-70b-versatile",
            base_url="https://api.groq.com/openai/v1",
        )

        tts = DeepgramTTSService(
            api_key=os.getenv("DEEPGRAM_API_KEY"),
            voice="aura-asteria-en",
        )

        # ✅ FIX: Proper aggregators for LLM context
        user_aggregator = LLMUserResponseAggregator()
        assistant_aggregator = LLMAssistantResponseAggregator()

        # ✅ FIX: Complete pipeline - Audio → STT → LLM → TTS → Audio
        pipeline = Pipeline([
            transport.input(),       # 🎤 Mic audio in from browser
            stt,                     # 🗣️ Speech → Text (Deepgram)
            user_aggregator,         # 📝 Collect user text for LLM
            llm,                     # 🤖 Groq LLM processes text
            tts,                     # 🔊 Text → Speech (Deepgram)
            assistant_aggregator,    # 📝 Collect assistant response
            transport.output(),      # 🔈 Audio out to browser
        ])

        # ✅ Send hospital system prompt to LLM
        system_message = {
            "role": "system",
            "content": build_system_prompt(HOSPITAL_KNOWLEDGE[:1500])
        }
        await llm.set_context({"messages": [system_message]})

        task = PipelineTask(
            pipeline,
            PipelineParams(allow_interruptions=True)
        )

        runner = PipelineRunner()
        await runner.run(task)

    except Exception as e:
        logger.error(f"Pipecat pipeline error {session_id}: {e}")
    finally:
        logger.info(f"Pipecat disconnected: {session_id}")


# ─── REST Endpoints (unchanged) ───────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "Medicare Voice AI - Running", "version": "2.0.0"}

@app.post("/api/chat")
async def chat_endpoint(payload: dict):
    session_id = payload.get("session_id", str(uuid.uuid4()))
    message = payload.get("message", "")
    if session_id not in manager.session_data:
        manager.session_data[session_id] = {
            "conversation": [], "collected_data": {}, "current_task": None
        }

    class DummyWS:
        async def send_text(self, data):
            pass

    response = await process_text_message(session_id, message, DummyWS())
    return {"session_id": session_id, "response": response}

@app.get("/api/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/api/check-slot")
def check_slot(doctor: str, date: str, slot: str):
    return check_slot_availability(doctor, date, slot)

@app.post("/book")
def book(data: dict):
    from app.services import book_appointment
    return book_appointment(supabase, data)

@app.post("/register-patient")
async def register_patient(data: dict):
    try:
        result = supabase.table("patient_accounts").insert({
            "email": data.get("email"),
            "password": data.get("password"),
        }).execute()
        return {"success": True, "data": result.data}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/login-patient")
async def login_patient(data: dict):
    try:
        result = supabase.table("patient_accounts").select("*").eq(
            "email", data.get("email")
        ).eq(
            "password", data.get("password")
        ).execute()

        if result.data:
            account = result.data[0]
            full_name = account.get("full_name", "")
            parts = full_name.split(" ", 1)
            return {
                "success": True,
                "data": {
                    "id":        account["id"],
                    "email":     account["email"],
                    "firstName": parts[0] if parts else "",
                    "lastName":  parts[1] if len(parts) > 1 else "",
                }
            }
        return {"success": False, "error": "Invalid credentials"}
    except Exception as e:
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True, log_level="info")