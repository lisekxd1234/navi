# NaviUnlock Pro – uruchamianie panelu

## Wymagania
- Python 3.11+
- Windows (instrukcja korzysta z PowerShella)

## Pierwsze uruchomienie
```powershell
cd C:\Users\jakub.lis\Documents\firma
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Start serwera
```powershell
cd C:\Users\jakub.lis\Documents\firma
.\.venv\Scripts\Activate.ps1
set FLASK_APP=app.app
set FLASK_ENV=development
flask run
```

Po starcie wejdź w przeglądarce na `http://127.0.0.1:5000`.

## Przydatne komendy
- Reset bazy (opcjonalnie): usuń plik `instance/finance.db`, a potem uruchom aplikację – tabele zostaną utworzone ponownie.
- Aktualizacja zależności: `pip install -r requirements.txt --upgrade`.

## Struktura
- `app/app.py` – główna aplikacja Flask.
- `app/templates/` – szablony HTML (dashboard, dokumenty).
- `app/static/` – style i JS (Chart.js dla donuta).
