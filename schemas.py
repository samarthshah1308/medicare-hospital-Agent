from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import date

class AppointmentCreate(BaseModel):
    full_name: str
    age: int
    gender: str
    phone: str
    email: EmailStr
    department: str
    patient_type: str
    preferred_date: date
    time_slot: str
    notes: Optional[str] = None
    doctor_name: Optional[str] = None