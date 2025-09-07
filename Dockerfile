# Optional: containerize the app
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY streamlit_app.py ./
ENV PORT=8501 HOST=0.0.0.0
EXPOSE 8501
CMD streamlit run streamlit_app.py --server.address=${HOST} --server.port=${PORT}
