FROM python:3.9-slim

# 建立工作目錄
WORKDIR /app

# 安裝套件
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# 複製專案檔案
COPY . .

# 預設啟動 Prefect Server（含 UI）
CMD ["prefect", "server", "start", "--host", "0.0.0.0"]