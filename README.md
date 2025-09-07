# Meme Radar — Streamlit

App Python/Streamlit (nessun build JS) che mostra KPI, grafici e diagnostica.

## Avvio locale
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env  # opzionale, oppure esporta le variabili d'ambiente
streamlit run streamlit_app.py
```
Apri http://localhost:8501

## Variabili d'ambiente
- REFRESH_SEC: secondi per l'auto-refresh (default 60)
- PROXY_TICKET_USD: ticket medio per stimare il volume 24h se manca (default 150)
- HOST / PORT: bind address/porta (default 0.0.0.0:8501)

## Docker (opzionale)
```bash
docker build -t meme-radar-streamlit .
docker run -p 8501:8501 --env REFRESH_SEC=60 --env PROXY_TICKET_USD=150 meme-radar-streamlit
```

## Note
- Niente CORS: le richieste partono dal server Python.
- Fallback DEX Screener incluso, Birdeye opzionale.
