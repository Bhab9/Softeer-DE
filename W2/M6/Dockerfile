FROM python:3.10-slim

# 작업 디렉토리
WORKDIR /app

# 패키지 복사
COPY requirements.txt .

# 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt

# 노트북 파일 복사
COPY notebooks /app/notebooks

# Jupyter 포트
EXPOSE 8888

# Jupyter 실행
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
