from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

from data import generate_uber_ride_confirmation
from producer import send_to_kafka

app = FastAPI()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@app.get("/")
def booking_home(request: Request):
    return templates.TemplateResponse(request, "home.html")


@app.get("/book")
def book_ride(request: Request):
    ride = generate_uber_ride_confirmation()
    send_to_kafka(ride)
    return templates.TemplateResponse(request, "confirmation.html")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
