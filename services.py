from datetime import datetime, timedelta

TIME_SLOT_MAP = {
    "morning":   "09:00:00",
    "midday":    "11:00:00",
    "afternoon": "14:00:00",
    "evening":   "16:00:00",
}

def book_appointment(supabase, data):
    try:
        print("FULL REQUEST DATA:", data)

        doctor_id     = data.get("doctor_id") or data.get("doctorId")
        date          = data.get("appointment_date")
        time_slot     = data.get("appointment_time") or data.get("time_slot") or data.get("preferred_time_slot")
        patient_email = data.get("email", "").strip().lower()
        patient_phone = data.get("phone", "").strip()

        print("FINAL doctor_id:", doctor_id)
        print("FINAL time_slot:", time_slot)

        if not doctor_id or not date or not time_slot:
            return {"error": "Missing required fields"}

        normalized_time = TIME_SLOT_MAP.get(str(time_slot).lower(), time_slot)

        try:
            selected = datetime.strptime(f"{date} {normalized_time}", "%Y-%m-%d %H:%M:%S")
            if selected < datetime.now():
                return {"error": "Past time not allowed"}
        except ValueError:
            return {"error": f"Invalid date/time format: {date} {normalized_time}"}

        existing = (
            supabase.table("appointments")
            .select("id")
            .eq("doctor_id", doctor_id)
            .eq("appointment_date", date)
            .eq("appointment_time", normalized_time)
            .execute()
        )
        if existing.data:
            next_time = (selected + timedelta(hours=1)).time()
            return {
                "error": "Slot already booked",
                "suggested_time": str(next_time),
            }

        # ─── Patient lookup ───────────────────────────────────────────────────
        patient_id = None

        if patient_phone:
            existing_by_phone = (
                supabase.table("patients")
                .select("id")
                .eq("phone", patient_phone)
                .execute()
            )
            if existing_by_phone.data:
                patient_id = existing_by_phone.data[0]["id"]
                print("EXISTING PATIENT BY PHONE:", patient_id)

        if not patient_id and patient_email:
            existing_by_email = (
                supabase.table("patients")
                .select("id")
                .eq("email", patient_email)
                .execute()
            )
            if existing_by_email.data:
                patient_id = existing_by_email.data[0]["id"]
                print("EXISTING PATIENT BY EMAIL:", patient_id)

        if not patient_id:
            patient_data = {
                "full_name": data.get("guest_name", ""),
                "age":       int(data.get("age")) if data.get("age") else None,
                "gender":    data.get("gender", ""),
                "phone":     patient_phone or None,
                "email":     patient_email or None,
            }
            try:
                new_patient = supabase.table("patients").insert(patient_data).execute()
                if new_patient.data:
                    patient_id = new_patient.data[0]["id"]
                    print("NEW PATIENT INSERTED:", patient_id)
            except Exception as pe:
                print("PATIENT INSERT ERROR (ignored):", str(pe))

        # ─── ✅ FIX: account_id lookup from patient_accounts by email ─────────
        user_id = data.get("user_id") or data.get("userId")

        # જો frontend એ user_id ન મોકલ્યો, તો email થી patient_accounts માં શોધો
        if not user_id and patient_email:
            try:
                account_lookup = (
                    supabase.table("patient_accounts")
                    .select("id")
                    .eq("email", patient_email)
                    .execute()
                )
                if account_lookup.data:
                    user_id = account_lookup.data[0]["id"]
                    print("ACCOUNT FOUND BY EMAIL:", user_id)
                else:
                    print("NO ACCOUNT FOUND FOR EMAIL:", patient_email)
            except Exception as ae:
                print("ACCOUNT LOOKUP ERROR (ignored):", str(ae))

        booking_type = "registered" if user_id else "guest"

        insert_data = {
            "doctor_id":        doctor_id,
            "appointment_date": date,
            "appointment_time": normalized_time,
            "time_slot":        str(time_slot).lower(),
            "patient_type":     data.get("patient_type", "new"),
            "booking_type":     booking_type,
            "notes":            data.get("notes", ""),
            "status":           "pending",
        }

        # ✅ FIX: booked_by_account_id ત્યારે જ add કરો જ્યારે valid user_id હોય
        # નહીં તો foreign key error આવે
        if user_id:
            insert_data["booked_by_account_id"] = user_id

        if patient_id:
            insert_data["patient_id"] = patient_id

        print("INSERT DATA:", insert_data)

        response = (
            supabase.table("appointments")
            .insert(insert_data)
            .execute()
        )

        print("INSERT SUCCESS:", response.data)

        return {
            "success":    True,
            "message":    "Appointment booked successfully",
            "data":       response.data,
            "patient_id": patient_id,
        }

    except Exception as e:
        print("INSERT ERROR:", str(e))
        return {"error": "Server error", "details": str(e)}
    
    