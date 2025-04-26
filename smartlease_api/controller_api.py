from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional

# Import all pipeline logic
from add_properties_and_poi.controller import run_pipeline
from add_properties_form.form_upsert import upsert_single_property
from smartlease_api.metadata_extractor import extract_metadata
from smartlease_api.hybrid_search import run_hybrid_search
from smartlease_api.property_ranker import rerank_with_llm
from smartlease_api.json_logger import save_step_data, clear_temp_logs

# ✅ Create ONE FastAPI app
app = FastAPI()

# --- Pipeline 1: Run full property pipeline ---
class PipelineRequest(BaseModel):
    location: str
    listing_type: str
    past_days: int
    start_row: int
    end_row: int

@app.post("/run-property-pipeline")
def run_full_pipeline(request: PipelineRequest):
    result = run_pipeline(
        location=request.location,
        listing_type=request.listing_type,
        past_days=request.past_days,
        start_row=request.start_row,
        end_row=request.end_row
    )
    return {"message": result}


# --- Pipeline 2: Add property via form ---
@app.post("/add-property-form")
async def add_property_form(
    property_id: str = Form(...),
    address: str = Form(...),
    status: str = Form(...),
    beds: str = Form(...),
    baths: str = Form(...),
    sqft: str = Form(...),
    year_built: str = Form(...),
    list_price: str = Form(...),
    nearby_schools: str = Form(...),
    primary_photo: Optional[UploadFile] = File(None),
    alt_photo: Optional[UploadFile] = File(None)
):
    try:
        property_data = {
            "property_id": property_id,
            "address": address,
            "status": status,
            "style": "",
            "beds": beds,
            "full_baths": baths,
            "sqft": sqft,
            "year_built": year_built,
            "list_price": list_price,
            "latitude": "",
            "longitude": "",
            "neighborhoods": "",
            "county": "",
            "nearby_schools": nearby_schools,
            "restaurant_name": "",
            "restaurant_rating": "",
            "restaurant_address": "",
            "cafe_name": "",
            "cafe_rating": "",
            "cafe_address": "",
            "hospital_name": "",
            "hospital_rating": "",
            "hospital_address": "",
            "pharmacy_name": "",
            "pharmacy_rating": "",
            "pharmacy_address": "",
            "atm_name": "",
            "atm_rating": "",
            "atm_address": "",
            "bank_name": "",
            "bank_rating": "",
            "bank_address": ""
        }

        image1_bytes = await primary_photo.read() if primary_photo else None
        image2_bytes = await alt_photo.read() if alt_photo else None

        result = upsert_single_property(property_data, image1=image1_bytes, image2=image2_bytes)
        return JSONResponse(content=result, status_code=200 if result["status"] == "success" else 409)

    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)




# --- Pipeline 3: Hybrid Search ---
class SearchRequest(BaseModel):
    query: str
@app.post("/hybrid-search/")
async def hybrid_search(request: SearchRequest):
    user_query = request.query

    metadata = extract_metadata(user_query)
    save_step_data("metadata.json", {"user_query": user_query, "metadata": metadata})

    results = run_hybrid_search(user_query, metadata)

    # ✅ Case-insensitive embedding key removal
    clean_results = [
        {k: v for k, v in row.items() if k.lower() != "complete_property_details_embedding"}
        for row in results
    ]

    save_step_data("search_results.json", clean_results)

    final_results = rerank_with_llm(user_query, clean_results)
    save_step_data("final_results.json", final_results)

    # clear_temp_logs()

    return final_results

