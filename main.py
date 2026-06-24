from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from dotenv import load_dotenv
import os
from supabase import create_client

from .services import book_appointment

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)

@app.get("/")
def root():
    return {"message": "Medicare FastAPI backend running"}

@app.post("/book")
def book(data: dict):
    return book_appointment(supabase, data)

@app.post("/register-patient")
async def register_patient(data: dict):
    try:
        print("DATA RECEIVED:", data)
        result = supabase.table("patient_accounts").insert({
            "email": data.get("email"),
            "password": data.get("password"),
        }).execute()
        print("INSERT RESULT:", result)
        return {"success": True, "data": result.data}
    except Exception as e:
        print("ERROR:", str(e))
        return {"success": False, "error": str(e)}

# ✅ NEW: login-patient endpoint
@app.post("/login-patient")
async def login_patient(data: dict):
    try:
        email    = data.get("email", "").strip().lower()
        password = data.get("password", "")

        print("LOGIN ATTEMPT:", email)

        # patient_accounts માંથી email + password match કરો
        result = (
            supabase.table("patient_accounts")
            .select("id, email")
            .eq("email", email)
            .eq("password", password)
            .execute()
        )

        print("LOGIN RESULT:", result.data)

        if not result.data:
            return {"success": False, "error": "Invalid email or password"}

        account = result.data[0]

        return {
            "success": True,
            "data": {
                "id":        account["id"],      # ✅ patient_accounts નો real UUID
                "email":     account["email"],
                "firstName": email.split("@")[0],
                "lastName":  "",
            }
        }

    except Exception as e:
        print("LOGIN ERROR:", str(e))
        return {"success": False, "error": str(e)}